import importlib.util
import sys
import tempfile
import types
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path


def load_tmmmover_class():
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
    app_plugins._PluginBase = object
    sys.modules["app.plugins"] = app_plugins

    app_chain = types.ModuleType("app.chain")
    sys.modules["app.chain"] = app_chain

    app_chain_media = types.ModuleType("app.chain.media")
    app_chain_media.MediaChain = object
    sys.modules["app.chain.media"] = app_chain_media

    app_schemas = types.ModuleType("app.schemas")
    sys.modules["app.schemas"] = app_schemas

    app_schemas_types = types.ModuleType("app.schemas.types")
    app_schemas_types.MediaSource = types.SimpleNamespace(TMDB="themoviedb")
    app_schemas_types.MediaType = types.SimpleNamespace(MOVIE="movie", TV="tv")
    sys.modules["app.schemas.types"] = app_schemas_types

    app_sdk_utilities = types.ModuleType("app.sdk.utilities")
    app_sdk_utilities.SystemUtils = object
    sys.modules["app.sdk.utilities"] = app_sdk_utilities

    apscheduler = types.ModuleType("apscheduler")
    sys.modules["apscheduler"] = apscheduler

    apscheduler_triggers = types.ModuleType("apscheduler.triggers")
    sys.modules["apscheduler.triggers"] = apscheduler_triggers

    apscheduler_triggers_cron = types.ModuleType("apscheduler.triggers.cron")
    apscheduler_triggers_cron.CronTrigger = object
    sys.modules["apscheduler.triggers.cron"] = apscheduler_triggers_cron

    watchdog = types.ModuleType("watchdog")
    sys.modules["watchdog"] = watchdog

    watchdog_events = types.ModuleType("watchdog.events")
    watchdog_events.FileSystemEventHandler = object
    sys.modules["watchdog.events"] = watchdog_events

    watchdog_observers = types.ModuleType("watchdog.observers")
    watchdog_observers.Observer = object
    sys.modules["watchdog.observers"] = watchdog_observers

    watchdog_observers_polling = types.ModuleType("watchdog.observers.polling")
    watchdog_observers_polling.PollingObserver = object
    sys.modules["watchdog.observers.polling"] = watchdog_observers_polling

    module_path = Path(__file__).resolve().parents[1] / "plugins.v3" / "tmmmover" / "__init__.py"
    spec = importlib.util.spec_from_file_location("tmmmover_under_test", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.TMMMover


class TMMMoverSeriesCategoryTest(unittest.TestCase):
    def test_notification_overview_is_channel_safe(self):
        mover = load_tmmmover_class()

        self.assertEqual(
            mover._sanitize_notification_overview("  第一行\n第二行\x00\x0b  "),
            "第一行 第二行",
        )

        long_plot = "简介内容 " * 100
        self.assertEqual(
            mover._sanitize_notification_overview(long_plot),
            ("简介内容 " * 100).strip(),
        )

    def test_talk_show_is_classified_as_variety_before_mainland(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            nfo = tmp_path / "tvshow.nfo"
            nfo.write_text(
                """<?xml version="1.0" encoding="utf-8"?>
<tvshow>
  <title>脱口秀和Ta的朋友们</title>
  <country>中国</country>
  <genre>Talk Show</genre>
</tvshow>
""",
                encoding="utf-8",
            )

            mover = load_tmmmover_class()
            mover._default_series_path = str(tmp_path / "Series")

            self.assertEqual(
                mover._resolve_series_target_root(mover, nfo),
                tmp_path / "Series" / "综艺",
            )

    def test_reality_tv_korean_show_is_classified_as_variety(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            nfo = tmp_path / "tvshow.nfo"
            nfo.write_text(
                """<?xml version="1.0" encoding="utf-8"?>
<tvshow>
  <title>Running Man</title>
  <country>韩国</country>
  <genre>Comedy</genre>
  <genre>Reality TV</genre>
</tvshow>
""",
                encoding="utf-8",
            )

            mover = load_tmmmover_class()
            mover._default_series_path = str(tmp_path / "Series")

            self.assertEqual(
                mover._resolve_series_target_root(mover, nfo),
                tmp_path / "Series" / "综艺",
            )

    def test_notification_image_prefers_nested_fanart_thumb_over_poster(self):
        root = ET.fromstring(
            """<tvshow>
  <thumb aspect="poster">https://image.tmdb.org/t/p/original/poster.jpg</thumb>
  <fanart>
    <thumb>https://image.tmdb.org/t/p/original/fanart.jpg</thumb>
  </fanart>
</tvshow>"""
        )

        mover = load_tmmmover_class()

        self.assertEqual(
            mover._extract_notification_images(root),
            (
                "https://image.tmdb.org/t/p/original/fanart.jpg",
                "https://image.tmdb.org/t/p/original/poster.jpg",
            ),
        )


if __name__ == "__main__":
    unittest.main()
