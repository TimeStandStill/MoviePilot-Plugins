import importlib.util
import sys
import types
import unittest
from pathlib import Path


class PluginBase:
    """Minimal V3 plugin-base test double."""

    def __init__(self):
        self.saved_configs = []

    def update_config(self, config):
        self.saved_configs.append(config)
        return True


def load_report_class():
    sys.modules.setdefault("app", types.ModuleType("app"))

    app_sdk = types.ModuleType("app.sdk")
    sys.modules["app.sdk"] = app_sdk
    app_sdk_logging = types.ModuleType("app.sdk.logging")
    app_sdk_logging.logger = types.SimpleNamespace(
        info=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
        error=lambda *args, **kwargs: None,
        debug=lambda *args, **kwargs: None,
    )
    sys.modules["app.sdk.logging"] = app_sdk_logging

    app_plugins = types.ModuleType("app.plugins")
    app_plugins._PluginBase = PluginBase
    sys.modules["app.plugins"] = app_plugins

    apscheduler = types.ModuleType("apscheduler")
    sys.modules["apscheduler"] = apscheduler
    apscheduler_triggers = types.ModuleType("apscheduler.triggers")
    sys.modules["apscheduler.triggers"] = apscheduler_triggers
    apscheduler_triggers_cron = types.ModuleType("apscheduler.triggers.cron")
    apscheduler_triggers_cron.CronTrigger = object
    sys.modules["apscheduler.triggers.cron"] = apscheduler_triggers_cron

    module_path = Path(__file__).resolve().parents[1] / "plugins.v3" / "dailymediareport" / "__init__.py"
    spec = importlib.util.spec_from_file_location("dailymediareport_under_test", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.DailyMediaReport


class DailyMediaReportHeaderMigrationTest(unittest.TestCase):
    def test_migrates_only_the_legacy_default_header(self):
        report_class = load_report_class()
        report = report_class()
        report.init_plugin({"header_image_url": report.LEGACY_DEFAULT_HEADER_IMAGE})

        self.assertEqual(report._header_image_url, report.DEFAULT_HEADER_IMAGE)
        self.assertEqual(report.saved_configs[-1]["header_image_url"], report.DEFAULT_HEADER_IMAGE)

    def test_preserves_custom_header(self):
        report_class = load_report_class()
        report = report_class()
        custom_url = "https://images.example.com/custom-header.png"
        report.init_plugin({"header_image_url": custom_url})

        self.assertEqual(report._header_image_url, custom_url)
        self.assertEqual(report.saved_configs, [])


if __name__ == "__main__":
    unittest.main()
