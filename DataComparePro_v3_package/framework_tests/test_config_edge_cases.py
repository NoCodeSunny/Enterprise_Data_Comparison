# -*- coding: utf-8 -*-
"""tests/test_config_edge_cases.py – TCFG01–TCFG06  Config edge cases."""
from __future__ import annotations
import sys, os, tempfile, unittest
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pathlib import Path

class TCFG01_Empty_Config(unittest.TestCase):
    """load_config with None returns hardcoded defaults (no crash)."""
    def test_TCFG01(self):
        from data_compare.config.config_loader import load_config
        cfg = load_config(None)
        self.assertIn("input_sheet", cfg)
        self.assertIn("capabilities", cfg)

class TCFG02_Invalid_YAML_Format(unittest.TestCase):
    """Malformed YAML falls back to defaults without crashing."""
    def setUp(self): self._d=tempfile.TemporaryDirectory(); self.t=Path(self._d.name)
    def tearDown(self): self._d.cleanup()
    def test_TCFG02(self):
        from data_compare.config.config_loader import load_config
        bad = self.t/"bad.yaml"
        bad.write_text(": this is not valid yaml: {\n  unclosed brace")
        cfg = load_config(str(bad))
        # Should fall back to defaults
        self.assertIn("input_sheet", cfg)

class TCFG03_Conflicting_Capabilities(unittest.TestCase):
    """validate_config accepts partial capabilities dict (rest use defaults)."""
    def test_TCFG03(self):
        from data_compare.config.config_schema import validate_config
        cfg = {"input_sheet":"/tmp/x.xlsx","report_root":"/tmp/r",
               "email_to":"t@t.com",
               "capabilities":{"comparison":True,"alerts":True}}
        result = validate_config(cfg)
        self.assertTrue(result["capabilities"]["comparison"])

class TCFG04_Invalid_Engine_Setting(unittest.TestCase):
    """Non-boolean use_spark raises ConfigError."""
    def test_TCFG04(self):
        from data_compare.config.config_schema import validate_config, ConfigError
        cfg = {"input_sheet":"/tmp/x.xlsx","report_root":"/tmp/r",
               "email_to":"t@t.com","use_spark":"maybe"}
        with self.assertRaises(ConfigError):
            validate_config(cfg)

class TCFG05_Missing_Required_Fields(unittest.TestCase):
    """Config without input_sheet raises ConfigError."""
    def test_TCFG05(self):
        from data_compare.config.config_schema import validate_config, ConfigError
        with self.assertRaises(ConfigError) as cm:
            validate_config({"report_root":"/tmp/r","email_to":"t@t.com"})
        self.assertIn("input_sheet", str(cm.exception))

class TCFG06_Extra_Unknown_Fields(unittest.TestCase):
    """Unknown top-level keys pass through without error."""
    def test_TCFG06(self):
        from data_compare.config.config_schema import validate_config
        cfg = {"input_sheet":"/tmp/x.xlsx","report_root":"/tmp/r",
               "email_to":"t@t.com","my_custom_field":"hello","another":42}
        result = validate_config(cfg)
        self.assertEqual(result["my_custom_field"], "hello")

if __name__ == "__main__":
    import unittest; unittest.main(verbosity=2)
