"""使用 MoviePilot V3 测试引导验证插件，不连接真实网盘

运行：python scripts/validate_p115_v3.py --host /path/to/MoviePilot
环境需安装宿主、插件 pyproject.toml 依赖及 pytest
"""

import argparse
import importlib
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


def validate(host: Path) -> bool:
    """在宿主隔离数据库和运行时中执行插件验证"""
    repository = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(host))
    spec = importlib.util.spec_from_file_location(
        "moviepilot_test_harness", host / "tests/conftest.py"
    )
    harness = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(harness)
    runtime_fixture = harness.configure_plugin_system_services.__wrapped__()
    next(runtime_fixture)

    import app.plugins

    app.plugins.__path__.insert(0, str(repository / "plugins.v3"))
    plugin = importlib.import_module("app.plugins.p115strmhelper")
    from app.db.models.transferhistory import TransferHistory
    from app.db.session import SessionFactory
    from app.schemas.types import MediaSource, MediaType
    from app.sdk.config import settings
    from app.chain.transfer import TransferChain
    from app.plugins.p115strmhelper.core.config import configer
    from app.plugins.p115strmhelper.db_manager.moviepilot_transfer import TransferHBOper
    from app.plugins.p115strmhelper.helper.mediasyncdel import MediaSyncDelHelper
    from app.plugins.p115strmhelper.helper.strm.transfer import TransferStrmHelper
    from app.plugins.p115strmhelper.patch.transfer_chain import TransferChainPatcher
    from app.plugins.p115strmhelper.service.one_shot import schedule_plugin_one_shot

    class V3IntegrationTests(unittest.TestCase):
        """验证真实 V3 类型、数据库、生命周期和调度接口"""

        def test_plugin_lifecycle_and_routes(self):
            """验证插件构造、数据库迁移、禁用状态初始化和路由声明"""
            instance = plugin.P115StrmHelper()
            self.assertEqual(instance.plugin_version, "3.0.0")
            instance.init_plugin({"enabled": False, "error_info_upload": False})
            self.assertGreater(len(instance.get_api()), 50)
            self.assertFalse(instance.get_state())
            self.assertTrue(Path(configer.PLUGIN_DB_PATH).is_file())
            self.assertTrue(plugin.Api.get_config_api()["native_transfer"])
            instance.stop_service()

        def test_native_transfer_ownership(self):
            """验证兼容入口不会替换 V3 整理执行方法"""
            original = TransferChain._TransferChain__handle_transfer
            TransferChainPatcher.enable(None, None, "115网盘Plus")
            self.assertIs(TransferChain._TransferChain__handle_transfer, original)
            TransferChainPatcher.disable()

        def test_transfer_history_identity_and_session(self):
            """验证新媒体身份查询和托管会话路径匹配"""
            movie_path = "/Movies/Example Movie (2026)/Example.mkv"
            with SessionFactory() as session:
                session.add(TransferHistory(
                    src=movie_path, dest=movie_path, src_storage="u115",
                    dest_storage="u115", title="Example Movie", year="2026",
                    type=MediaType.MOVIE.value, status=True,
                    media_source=MediaSource.TMDB.value, media_id="12345",
                ))
                session.commit()
            helper = MediaSyncDelHelper()
            configer.sync_del_remove_versions = False
            _, records = helper._MediaSyncDelHelper__get_transfer_his(
                "Movie", "Example Movie", movie_path, 12345, None, None
            )
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].media_id, "12345")
            matches = TransferHBOper().get_transfer_his_by_path_title("Example Movie")
            self.assertTrue(any(row.media_id == "12345" for row in matches))

        def test_strm_file_generation(self):
            """验证目录映射、STRM 文件内容和本地写入"""
            with tempfile.TemporaryDirectory() as directory:
                success, generated = TransferStrmHelper().generate_strm_files(
                    target_dir=directory, pan_media_dir="/Movies",
                    item_dest_path=Path("/Movies/Example (2026)/Example.mkv"),
                    url="https://example.test/redirect?pickcode=abc123",
                )
                self.assertTrue(success)
                self.assertEqual(Path(generated).suffix, ".strm")
                self.assertEqual(
                    Path(generated).read_text(encoding="utf-8"),
                    "https://example.test/redirect?pickcode=abc123",
                )

        def test_scheduler_sdk(self):
            """验证一次性任务只使用宿主公开调度接口"""
            with patch(
                "app.plugins.p115strmhelper.service.one_shot.add_plugin_once_job",
                return_value=True,
            ) as register:
                callback = lambda: None
                self.assertTrue(schedule_plugin_one_shot("example", "测试", callback))
                register.assert_called_once_with(
                    plugin_id="P115StrmHelper",
                    job_id="P115StrmHelper_example", func=callback, name="测试",
                    delay_seconds=3, func_kwargs={},
                )

    suite = unittest.defaultTestLoader.loadTestsFromTestCase(V3IntegrationTests)
    source = repository / "plugins.v3/p115strmhelper"
    sys.path.append(str(source))
    for name in ("path_utils", "time_utils", "url_utils", "cron_utils", "exception_utils"):
        test_spec = importlib.util.spec_from_file_location(
            f"upstream_test_{name}", source / "tests" / f"test_{name}.py"
        )
        module = importlib.util.module_from_spec(test_spec)
        test_spec.loader.exec_module(module)
        suite.addTests(unittest.defaultTestLoader.loadTestsFromModule(module))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    print(f"Host config isolated at {settings.CONFIG_PATH}", flush=True)
    return result.wasSuccessful()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True, type=Path)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    arguments = parser.parse_args()
    if arguments.worker:
        try:
            status = 0 if validate(arguments.host.resolve()) else 1
        except Exception:
            import traceback
            traceback.print_exc()
            status = 1
        # 宿主测试引导会创建后台线程，子进程退出避免测试命令等待其常驻线程
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(status)
    completed = subprocess.run([
        sys.executable, "-X", "utf8", str(Path(__file__).resolve()),
        "--host", str(arguments.host.resolve()), "--worker",
    ], timeout=120)
    raise SystemExit(completed.returncode)
