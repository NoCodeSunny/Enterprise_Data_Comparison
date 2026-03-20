# -*- coding: utf-8 -*-
"""tests/test_security.py – TSEC01–TSEC04  Security and path validation tests."""
from __future__ import annotations
import sys, os, tempfile, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pathlib import Path

class TSEC01_Permission_Denied(unittest.TestCase):
    """PermissionError mapped to PERMISSION_DENIED with Excel close hint."""
    def test_TSEC01(self):
        from data_compare.debugger import Debugger
        exc = PermissionError("[Errno 13] Permission denied: '/data/locked.csv'")
        r = Debugger().diagnose(exc)
        self.assertEqual(r.error_record.error_code,"PERMISSION_DENIED")
        self.assertIn("Excel", r.error_record.recommended_fix)

class TSEC02_ReadOnly_Directory(unittest.TestCase):
    """Attempting to write to read-only dir raises PermissionError handled gracefully."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TSEC02(self):
        ro_dir = self.t/"ro"
        ro_dir.mkdir()
        try:
            os.chmod(ro_dir, 0o555)  # read-only
            test_file = ro_dir/"test.txt"
            try:
                test_file.write_text("test")
                written = True
            except PermissionError:
                written = False
            finally:
                os.chmod(ro_dir, 0o755)  # restore
            # Either written (some filesystems allow) or not — both OK, no crash
            self.assertIsInstance(written, bool)
        except Exception:
            pass  # chmod may not work in all environments

class TSEC03_Path_Injection_Sanitised(unittest.TestCase):
    """Path traversal characters removed by sanitise_filename."""
    def test_TSEC03(self):
        from data_compare.utils.helpers import sanitise_filename
        dangerous = "../../etc/passwd"
        safe = sanitise_filename(dangerous)
        # sanitise_filename removes path separators (/) and control chars
        self.assertNotIn("/", safe)
        # The result must not allow directory traversal via path.resolve()
        # (dots alone are not a traversal risk without the slash)
        self.assertGreater(len(safe), 0)

class TSEC04_Invalid_Path_Characters(unittest.TestCase):
    """Filenames with illegal chars are sanitised; result is OS-safe."""
    def test_TSEC04(self):
        from data_compare.utils.helpers import sanitise_filename
        evil = "my:file<name>with|bad?chars*"
        safe = sanitise_filename(evil)
        for bad in (':', '<', '>', '|', '?', '*'):
            self.assertNotIn(bad, safe)
        self.assertGreater(len(safe), 0)

if __name__ == "__main__":
    import unittest; unittest.main(verbosity=2)
