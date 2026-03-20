# -*- coding: utf-8 -*-
"""
data_compare.capabilities.plugins.plugin_capability
─────────────────────────────────────────────────────
PluginCapability

Dynamically discovers and executes user-defined plugin functions.

Plugins are configured in config.yaml under a "plugins" key:

    plugins:
      enabled: true
      modules:
        - "my_project.plugins.custom_check"
        - "my_project.plugins.export_to_s3"

Each plugin module must expose a function:

    def run(context: dict) -> dict:
        # read from context
        # do custom work
        # write results into context["plugin_outputs"]["<plugin_name>"]
        return context

Plugins that fail are logged but do not stop the pipeline.
Plugin outputs are stored in context['plugin_outputs'][module_name].

Requires context keys
---------------------
    config.plugins  : dict with keys:
        enabled : bool
        modules : list[str]   – fully-qualified module paths

Writes context keys
-------------------
    plugin_outputs : dict {module_name: any}

Fully independent of all other capabilities.
"""

from __future__ import annotations

import importlib
from typing import Any, Dict

from data_compare.capabilities.base import BaseCapability
from data_compare.utils.logger import get_logger

logger = get_logger(__name__)


class PluginCapability(BaseCapability):
    NAME = "plugins"

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        plugin_cfg = context.get("config", {}).get("plugins", {})

        if not plugin_cfg:
            logger.debug("  [plugins] No plugin config found – skipping")
            return context

        if not plugin_cfg.get("enabled", False):
            logger.debug("  [plugins] Plugins disabled in config – skipping")
            return context

        modules: list = plugin_cfg.get("modules", [])
        if not modules:
            logger.info("  [plugins] No plugin modules listed – nothing to run")
            return context

        logger.info(f"  [plugins] Running {len(modules)} plugin(s)")

        for module_path in modules:
            module_path = str(module_path).strip()
            if not module_path:
                continue
            try:
                mod = importlib.import_module(module_path)
                if not hasattr(mod, "run"):
                    logger.warning(
                        f"  [plugins] Module '{module_path}' has no run() function – skipped"
                    )
                    continue
                logger.info(f"  [plugins] Executing plugin: {module_path}")
                context = mod.run(context)
                logger.info(f"  [plugins] Plugin '{module_path}' completed")
            except ImportError as exc:
                logger.warning(
                    f"  [plugins] Cannot import '{module_path}': {exc} – skipped"
                )
            except Exception as exc:
                logger.error(
                    f"  [plugins] Plugin '{module_path}' raised an exception: {exc}",
                    exc_info=True,
                )
                # Do not re-raise – plugin failure must not stop the pipeline

        return context
