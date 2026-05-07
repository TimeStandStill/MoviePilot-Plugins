import re
import shutil
import threading
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from apscheduler.triggers.cron import CronTrigger
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

from app.log import logger
from app.plugins import _PluginBase
from app.chain.tmdb import TmdbChain
from app.schemas.types import MediaType
from app.utils.system import SystemUtils


_LINK_LOCK = threading.Lock()


class TMMLinkMonitorHandler(FileSystemEventHandler):
    def __init__(self, monpath: str, plugin: "TMMMover", **kwargs):
        super().__init__(**kwargs)
        self._watch_path = monpath
        self._plugin = plugin

    def on_created(self, event):
        self._plugin._link_event_handler(event=event, mon_path=self._watch_path, event_path=event.src_path)

    def on_moved(self, event):
        self._plugin._link_event_handler(event=event, mon_path=self._watch_path, event_path=event.dest_path)


class TMMMover(_PluginBase):
    """
    TMM 元数据智能转移助手
    """

    plugin_name = "TMM 元数据转移助手"
    plugin_desc = (
        "根据 TMM NFO 自动分拣迁移，并精准提取 TMDB 数据模拟原生图文入库通知"
    )
    plugin_version = "2.1.2"
    plugin_author = "QB"
    author_url = "https://github.com/TimeStandStill/MoviePilot-Plugins"
    plugin_icon = "sync.png"
    plugin_order = 66

    SERIES_CATEGORIES = {
        "mainland": "大陆剧集",
        "anime": "动漫",
        "shortdrama": "短剧",
        "hktw": "港台剧集",
        "documentary": "纪录片",
        "western": "欧美剧集",
        "jpkr": "日韩剧集",
        "variety": "综艺",
    }

    # TMDB 标准电影流派汉化字典
    GENRE_MAPPING = {
        "action": "动作", "adventure": "冒险", "animation": "动画",
        "comedy": "喜剧", "crime": "犯罪", "documentary": "纪录片",
        "drama": "剧情", "family": "家庭", "fantasy": "奇幻",
        "history": "历史", "horror": "恐怖", "music": "音乐",
        "mystery": "悬疑", "romance": "爱情", "science fiction": "科幻",
        "sci-fi": "科幻", "tv movie": "电视电影", "thriller": "惊悚",
        "war": "战争", "western": "西部"
    }
    WECOM_OVERVIEW_MAX_LEN = 42
    WECOM_OVERVIEW_ELLIPSIS = "..."
    WECOM_PRIMARY_IMAGE_ASPECTS = ("fanart", "backdrop")
    WECOM_SECONDARY_IMAGE_ASPECTS = ("banner", "landscape")
    MEDIA_EXTENSIONS = {".mp4", ".mkv", ".ts", ".avi", ".rmvb", ".wmv", ".iso", ".m2ts"}
    TEMP_DOWNLOAD_SUFFIXES = (".!qb", ".part", ".parts", ".tmp", ".qb!")

    def __init__(self):
        super().__init__()
        self._enabled: bool = False
        self._source_movie_path: str = ""
        self._source_series_path: str = ""
        self._default_movie_path: str = ""
        self._default_series_path: str = ""
        self._cron: str = ""
        self._notify_enabled: bool = False
        self._link_enabled: bool = False
        self._link_notify: bool = False
        self._link_onlyonce: bool = False
        self._link_mode: str = "fast"
        self._link_size: float = 0
        self._link_quiet_seconds: int = 300
        self._link_exclude_keywords: str = ""
        self._link_source_movie_path: str = ""
        self._link_source_series_path: str = ""
        self._move_quiet_minutes: int = 10
        self._observer: List[Any] = []
        self._dirconf: Dict[str, Path] = {}
        self._link_pending: Set[str] = set()

    def init_plugin(self, config: dict = None):
        config = config or {}
        self.stop_service()

        self._source_movie_path = (config.get("source_movie_path") or "").strip()
        self._source_series_path = (config.get("source_series_path") or "").strip()
        self._default_movie_path = (config.get("default_movie_path") or "").strip()
        self._default_series_path = (config.get("default_series_path") or "").strip()
        self._cron = (config.get("cron") or "").strip()
        self._notify_enabled = config.get("notify_enabled", False)
        self._link_enabled = config.get("link_enabled", False)
        self._link_notify = config.get("link_notify", False)
        self._link_onlyonce = config.get("link_onlyonce", False)
        self._link_mode = (config.get("link_mode") or "fast").strip() or "fast"
        self._link_size = float(config.get("link_size") or 0)
        self._link_quiet_seconds = max(0, int(config.get("link_quiet_seconds") or 300))
        self._link_exclude_keywords = (config.get("link_exclude_keywords") or "").strip()
        self._link_source_movie_path = (config.get("link_source_movie_path") or "").strip()
        self._link_source_series_path = (config.get("link_source_series_path") or "").strip()
        self._move_quiet_minutes = max(0, int(config.get("move_quiet_minutes") or 10))

        movie_ready = bool(self._source_movie_path and self._default_movie_path)
        series_ready = bool(self._source_series_path and self._default_series_path)
        move_ready = bool(movie_ready or series_ready)
        link_ready = bool(
            (self._link_source_movie_path and self._source_movie_path)
            or (self._link_source_series_path and self._source_series_path)
        )
        self._enabled = bool(move_ready or (self._link_enabled and link_ready) or (self._link_onlyonce and link_ready))

        self._dirconf = {}
        self._register_link_pair(self._link_source_movie_path, self._source_movie_path, "电影")
        self._register_link_pair(self._link_source_series_path, self._source_series_path, "剧集")

        if self._link_enabled:
            self._start_link_observers()

        if self._link_onlyonce and self._dirconf:
            threading.Thread(target=self._run_link_sync_once, daemon=True).start()

        logger.info(
            f"【TMM转移助手】配置加载: 启用={self._enabled}, 通知={self._notify_enabled}, "
            f"硬链接监控={self._link_enabled}, 监控目录数={len(self._dirconf)}"
        )

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [
            {"cmd": "plugin/TMMMover/run", "method": "post", "text": "立即运行迁移", "icon": "mdi-play", "color": "primary"},
            {"cmd": "plugin/TMMMover/sync_links", "method": "post", "text": "立即同步硬链接", "icon": "mdi-link-variant", "color": "info"},
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {"path": "/run", "endpoint": self.api_run_once, "auth": "bear", "methods": ["POST"], "summary": "手动触发 TMM 转移任务"},
            {"path": "/sync_links", "endpoint": self.api_sync_links_once, "auth": "bear", "methods": ["POST"], "summary": "手动触发实时硬链接全量同步"},
        ]

    def get_form(self) -> Tuple[Optional[List[dict]], Dict[str, Any]]:
        form = [{"component": "VForm", "content": [
            {"component": "VTabs", "props": {"model": "active_tab", "color": "primary", "grow": True}, "content": [
                {"component": "VTab", "props": {"value": "link"}, "text": "实时硬链接"},
                {"component": "VTab", "props": {"value": "move"}, "text": "TMM 转移"},
            ]},
            {"component": "VWindow", "props": {"model": "active_tab", "class": "mt-4"}, "content": [
                {"component": "VWindowItem", "props": {"value": "link"}, "content": [
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "link_enabled", "label": "启用实时硬链接", "color": "primary"}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "link_notify", "label": "硬链接发送通知", "color": "info"}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [{"component": "VSwitch", "props": {"model": "link_onlyonce", "label": "保存后全量补链一次", "color": "warning"}}]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VPathField", "props": {"model": "link_source_movie_path", "label": "电影下载目录 A", "placeholder": "/downloads/Movies", "persistent-hint": True}}]},
                        {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VPathField", "props": {"model": "source_movie_path", "label": "电影 TMM 刮削目录 B", "placeholder": "/media/source/Movies", "persistent-hint": True}}]},
                        {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VPathField", "props": {"model": "link_source_series_path", "label": "剧集下载目录 A", "placeholder": "/downloads/Series", "persistent-hint": True}}]},
                        {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VPathField", "props": {"model": "source_series_path", "label": "剧集 TMM 刮削目录 B", "placeholder": "/media/source/Series", "persistent-hint": True}}]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VSelect", "props": {"model": "link_mode", "label": "硬链接监控模式", "items": [{"title": "性能模式", "value": "fast"}, {"title": "兼容模式", "value": "compatibility"}]}}]},
                        {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "link_size", "label": "最小硬链接文件大小（KB）", "placeholder": "小于该值改为复制"}}]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [{"component": "VTextField", "props": {"model": "link_quiet_seconds", "label": "下载完成保护秒数", "placeholder": "300", "hint": "文件最后修改时间静默达到该秒数后，才会生成硬链接", "persistent-hint": True}}]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VTextarea", "props": {"model": "link_exclude_keywords", "label": "硬链接排除关键词", "rows": 2, "placeholder": "每行一个正则，命中则跳过"}}]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VAlert", "props": {"type": "info", "variant": "tonal", "text": "这一页完全对应 MP 官方实时硬链接思路：只做实时监控和手动全量补链，不做定时任务；同时会等待下载文件静默一段时间后再生成硬链接，尽量避开 QB 未完成文件。"}}]},
                    ]},
                ]},
                {"component": "VWindowItem", "props": {"value": "move"}, "content": [
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VPathField", "props": {"model": "default_movie_path", "label": "目标电影存放目录", "placeholder": "/media/movies"}}]},
                        {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VPathField", "props": {"model": "default_series_path", "label": "目标剧集存放根目录", "placeholder": "/media/series", "persistent-hint": True}}]},
                        {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VCronField", "props": {"model": "cron", "label": "TMM 转移执行 Cron", "placeholder": "0 * * * *"}}]},
                        {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VTextField", "props": {"model": "move_quiet_minutes", "label": "入库前静默保护分钟数", "placeholder": "10", "hint": "目录内媒体文件若在该时间内仍有写入痕迹，则暂不入库", "persistent-hint": True}}]},
                        {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VSwitch", "props": {"model": "notify_enabled", "label": "启用精美入库通知", "color": "primary", "persistent-hint": True}}]},
                    ]},
                    {"component": "VRow", "content": [
                        {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VAlert", "props": {"type": "info", "variant": "tonal", "text": "这一页保留你原来的 TMM 转移逻辑：只处理已经在 B 目录刮削完成并带 NFO 的影视目录；入库前还会再次检查有没有最近仍在写入的媒体文件。"}}]},
                    ]},
                ]},
            ]},
        ]}]
        model = {
            "active_tab": "link",
            "link_enabled": False,
            "link_notify": False,
            "link_onlyonce": False,
            "link_source_movie_path": "",
            "source_movie_path": "",
            "link_source_series_path": "",
            "source_series_path": "",
            "link_mode": "fast",
            "link_size": 0,
            "link_quiet_seconds": 300,
            "link_exclude_keywords": "",
            "default_movie_path": "",
            "default_series_path": "",
            "cron": "0 * * * *",
            "move_quiet_minutes": 10,
            "notify_enabled": False,
        }
        return form, model

    def get_page(self) -> Optional[List[dict]]:
        return [
            {"component": "VCard", "props": {"variant": "outlined", "class": "mb-4"}, "content": [{"component": "VCardText", "props": {"class": "pa-6 d-flex flex-column align-center"}, "content": [
                {"component": "VIcon", "props": {"icon": "mdi-link-variant", "size": "64", "color": "info", "class": "mb-4"}},
                {"component": "div", "props": {"class": "text-h6 mb-2"}, "text": "手动同步硬链接"},
                {"component": "div", "props": {"class": "text-body-2 text-medium-emphasis mb-6 text-center"}, "text": "将下载目录 A 中的新文件按原相对路径同步到 TMM 刮削目录 B，适合首次接入或漏链补跑。"},
                {"component": "VBtn", "props": {"color": "info", "variant": "elevated", "size": "large", "prepend-icon": "mdi-link-plus"}, "text": "立即同步硬链接", "events": {"click": {"api": "plugin/TMMMover/sync_links", "method": "post"}}},
            ]}]},
            {"component": "VCard", "props": {"variant": "outlined", "class": "mb-4"}, "content": [{"component": "VCardText", "props": {"class": "pa-6 d-flex flex-column align-center"}, "content": [
                {"component": "VIcon", "props": {"icon": "mdi-folder-move", "size": "64", "color": "primary", "class": "mb-4"}},
                {"component": "div", "props": {"class": "text-h6 mb-2"}, "text": "手动执行迁移任务"},
                {"component": "div", "props": {"class": "text-body-2 text-medium-emphasis mb-6 text-center"}, "text": "只处理已经被 TMM 刮削完成的目录，严格遵照 NFO 数据转移入媒体库。"},
                {"component": "VBtn", "props": {"color": "primary", "variant": "elevated", "size": "large", "prepend-icon": "mdi-rocket-launch"}, "text": "立即运行迁移", "events": {"click": {"api": "plugin/TMMMover/run", "method": "post"}}},
            ]}]},
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        services: List[Dict[str, Any]] = []
        if self.get_state() and self._cron:
            try:
                services.append({"id": "scan_move_job", "name": "TMM 目录转移任务", "trigger": CronTrigger.from_crontab(self._cron), "func": self.run_once})
            except Exception:
                pass
        return services

    def stop_service(self):
        if self._observer:
            for observer in self._observer:
                try:
                    observer.stop()
                    observer.join()
                except Exception as e:
                    logger.debug(f"【TMM转移助手】停止硬链接监控失败: {e}")
        self._observer = []
        self._link_pending = set()

    def api_run_once(self) -> Dict[str, Any]:
        threading.Thread(target=self.run_once, daemon=True).start()
        return {"code": 0, "msg": "✅ 任务已在后台启动！"}

    def api_sync_links_once(self) -> Dict[str, Any]:
        threading.Thread(target=self.sync_all_links, daemon=True).start()
        return {"code": 0, "msg": "✅ 硬链接全量同步已在后台启动！"}

    def _run_link_sync_once(self):
        self.sync_all_links()
        self._link_onlyonce = False
        self.update_config({
            "link_enabled": self._link_enabled,
            "link_notify": self._link_notify,
            "link_onlyonce": self._link_onlyonce,
            "link_source_movie_path": self._link_source_movie_path,
            "source_movie_path": self._source_movie_path,
            "link_source_series_path": self._link_source_series_path,
            "source_series_path": self._source_series_path,
            "link_mode": self._link_mode,
            "link_size": self._link_size,
            "link_quiet_seconds": self._link_quiet_seconds,
            "link_exclude_keywords": self._link_exclude_keywords,
            "default_movie_path": self._default_movie_path,
            "default_series_path": self._default_series_path,
            "cron": self._cron,
            "move_quiet_minutes": self._move_quiet_minutes,
            "notify_enabled": self._notify_enabled,
        })

    def _register_link_pair(self, monitor_path: str, target_path: str, label: str):
        if not monitor_path and not target_path:
            return
        if not monitor_path or not target_path:
            logger.warning(f"【TMM转移助手】{label}硬链接配置不完整，已跳过")
            return
        self._dirconf[monitor_path] = Path(target_path)

    def _start_link_observers(self):
        for mon_path, target_path in self._dirconf.items():
            try:
                if target_path.is_relative_to(Path(mon_path)):
                    logger.warning(f"【TMM转移助手】{target_path} 是监控目录 {mon_path} 的子目录，无法启用实时硬链接")
                    continue
            except Exception:
                pass

            try:
                observer = PollingObserver(timeout=10) if self._link_mode == "compatibility" else Observer(timeout=10)
                observer.schedule(TMMLinkMonitorHandler(mon_path, self), path=mon_path, recursive=True)
                observer.daemon = True
                observer.start()
                self._observer.append(observer)
                logger.info(f"【TMM转移助手】实时硬链接监控已启动: {mon_path} -> {target_path}")
            except Exception as e:
                logger.error(f"【TMM转移助手】实时硬链接监控启动失败 [{mon_path}]: {e}")

    def sync_all_links(self):
        if not self._dirconf:
            logger.warning("【TMM转移助手】未配置实时硬链接目录映射，跳过全量同步")
            return
        logger.info("【TMM转移助手】=== 开始执行硬链接全量同步 ===")
        for mon_path in self._dirconf.keys():
            for file_path in SystemUtils.list_files(Path(mon_path), ['.*']):
                self._link_file_when_ready(event_path=str(file_path), mon_path=mon_path, require_quiet=True)
        logger.info("【TMM转移助手】=== 硬链接全量同步完成 ===")

    def _link_event_handler(self, event, mon_path: str, event_path: str):
        if not event.is_directory:
            self._handle_link_file(event_path=event_path, mon_path=mon_path)

    def _handle_link_file(self, event_path: str, mon_path: str):
        file_key = str(Path(event_path))
        with _LINK_LOCK:
            if file_key in self._link_pending:
                return
            self._link_pending.add(file_key)
        threading.Thread(
            target=self._wait_and_link_file,
            args=(event_path, mon_path),
            daemon=True
        ).start()

    def _wait_and_link_file(self, event_path: str, mon_path: str):
        try:
            check_interval = max(5, min(30, self._link_quiet_seconds // 3 or 5))
            while True:
                file_path = Path(event_path)
                if not file_path.exists():
                    return
                ready, reason = self._is_link_source_ready(file_path)
                if ready:
                    break
                logger.debug(f"【TMM转移助手】等待文件下载完成后再硬链接: {file_path.name} ({reason})")
                time.sleep(check_interval)
            self._link_file_when_ready(event_path=event_path, mon_path=mon_path, require_quiet=True)
        except Exception as e:
            logger.error(f"【TMM转移助手】硬链接等待异常 [{event_path}]: {e}")
        finally:
            with _LINK_LOCK:
                self._link_pending.discard(str(Path(event_path)))

    def _link_file_when_ready(self, event_path: str, mon_path: str, require_quiet: bool):
        file_path = Path(event_path)
        try:
            if not file_path.exists():
                return
            with _LINK_LOCK:
                if self._is_ignored_link_path(event_path):
                    return
                if self._match_link_exclude_keywords(event_path):
                    return
                if require_quiet:
                    ready, reason = self._is_link_source_ready(file_path)
                    if not ready:
                        logger.info(f"【TMM转移助手】跳过未完成文件 {file_path.name}：{reason}")
                        return

                transfer_type = "link"
                if self._link_size > 0 and file_path.stat().st_size < self._link_size * 1024:
                    transfer_type = "copy"

                target = self._dirconf.get(mon_path)
                if not target:
                    return

                state, errmsg = self._link_file(src_path=file_path, mon_path=mon_path, target_path=target, transfer_type=transfer_type)
                if not state:
                    logger.warning(f"【TMM转移助手】{file_path.name} 硬链接失败：{errmsg}")
                    if self._link_notify:
                        self.post_message(title=f"{file_path.name} 硬链接失败", text=f"原因：{errmsg or '未知'}")
                    return

                logger.info(f"【TMM转移助手】{file_path.name} {'复制' if transfer_type == 'copy' else '硬链接'}成功")
                if self._link_notify:
                    self.post_message(title=f"{file_path.name} 硬链接完成", text=f"目标目录：{target}")
        except Exception as e:
            logger.error(f"【TMM转移助手】硬链接处理异常 [{event_path}]: {e}")

    @classmethod
    def _is_ignored_link_path(cls, event_path: str) -> bool:
        return any(token in event_path for token in ('/@Recycle/', '/#recycle/', '/@eaDir')) or '/.' in event_path

    def _match_link_exclude_keywords(self, event_path: str) -> bool:
        if not self._link_exclude_keywords:
            return False
        for keyword in self._link_exclude_keywords.splitlines():
            keyword = keyword.strip()
            if keyword and re.findall(keyword, event_path):
                logger.info(f"【TMM转移助手】{event_path} 命中硬链接排除关键词 {keyword}，已跳过")
                return True
        return False

    @classmethod
    def _is_temp_download_file(cls, file_path: Path) -> bool:
        lower_name = file_path.name.lower()
        return any(lower_name.endswith(suffix) for suffix in cls.TEMP_DOWNLOAD_SUFFIXES)

    def _is_link_source_ready(self, file_path: Path) -> Tuple[bool, str]:
        if not file_path.exists():
            return False, "源文件不存在"
        if not file_path.is_file():
            return False, "不是文件"
        if self._is_temp_download_file(file_path):
            return False, "仍是下载临时文件"

        quiet_seconds = max(0, self._link_quiet_seconds)
        if quiet_seconds <= 0:
            return True, "已关闭保护"

        file_age = time.time() - file_path.stat().st_mtime
        if file_age < quiet_seconds:
            return False, f"最近 {int(file_age)} 秒内仍有写入"
        return True, "已静默"

    @staticmethod
    def _link_file(src_path: Path, mon_path: str, target_path: Path, transfer_type: str = "link") -> Tuple[bool, str]:
        try:
            rel_path = src_path.relative_to(Path(mon_path))
        except ValueError:
            return False, "文件路径不在监控目录内"

        new_path = target_path / rel_path
        if new_path.exists():
            return True, "目标路径文件已存在"

        if not new_path.parent.exists():
            new_path.parent.mkdir(parents=True, exist_ok=True)

        if transfer_type == "copy":
            code, errmsg = SystemUtils.copy(src_path, new_path)
        else:
            code, errmsg = SystemUtils.link(src_path, new_path)
        return (code == 0), errmsg

    @classmethod
    def _truncate_wecom_overview(cls, text: str) -> str:
        overview = re.sub(r"\s+", " ", (text or "").strip())
        if not overview:
            return "暂无简介"
        max_len = cls.WECOM_OVERVIEW_MAX_LEN
        if len(overview) <= max_len:
            return overview
        cutoff = max(0, max_len - len(cls.WECOM_OVERVIEW_ELLIPSIS))
        return overview[:cutoff].rstrip() + cls.WECOM_OVERVIEW_ELLIPSIS

    @classmethod
    def _extract_notification_images(cls, root: ET.Element) -> Tuple[str, str]:
        message_image = ""
        poster_image = ""
        secondary_image = ""

        for thumb in root.findall(".//thumb"):
            text = (thumb.text or "").strip()
            if not text.startswith("http"):
                continue

            aspect = (thumb.get("aspect") or "").strip().lower()
            if aspect == "poster" and not poster_image:
                poster_image = text
                continue
            if aspect in cls.WECOM_PRIMARY_IMAGE_ASPECTS and not message_image:
                message_image = text
                continue
            if aspect in cls.WECOM_SECONDARY_IMAGE_ASPECTS and not secondary_image:
                secondary_image = text

        if not message_image:
            message_image = secondary_image
        return message_image, poster_image

    def run_once(self) -> str:
        if not self.get_state(): return "未完全配置"
        logger.info(f"【TMM转移助手】=== 开始执行后台扫描任务 ===")
        movie_res = self._scan_source_dir(self._source_movie_path, "movie")
        series_res = self._scan_source_dir(self._source_series_path, "series")

        final_moved = len(movie_res["moved"]) + len(series_res["moved"])
        final_skipped = len(movie_res["skipped"]) + len(series_res["skipped"])
        final_errors = len(movie_res["errors"]) + len(series_res["errors"])

        logger.info(f"【TMM转移助手】=== 任务完成 === | 成功: {final_moved} | 跳过: {final_skipped} | 失败: {final_errors}")
        return "任务完成"

    def _scan_source_dir(self, source_path: str, mode: str) -> Dict[str, List[str]]:
        res = {"moved": [], "skipped": [], "errors": []}
        if not source_path: return res
        source_dir = Path(source_path)
        if not source_dir.exists() or not source_dir.is_dir(): return res

        for child in source_dir.iterdir():
            if not child.is_dir(): continue
            if child.name.endswith(".deletedByTMM") or child.name == ".deletedByTMM":
                try: shutil.rmtree(child)
                except: pass
                continue
            try:
                status = self._process_one_folder(child, mode)
                if status == "MOVED": res["moved"].append(child.name)
                elif status == "SKIPPED": res["skipped"].append(child.name)
                elif status == "ERROR": res["errors"].append(child.name)
            except Exception as e:
                res["errors"].append(child.name)
                logger.error(f"【TMM转移助手】处理失败 [{child.name}]: {str(e)}")
        return res

    def _has_year_in_name(self, folder_name: str) -> bool:
        return bool(re.search(r"[\(（]\d{4}[\)）]", folder_name))

    def _process_one_folder(self, folder: Path, mode: str) -> str:
        if not self._has_year_in_name(folder.name): return "SKIPPED"
        nfo_files = list(folder.glob("*.nfo"))
        if not nfo_files: return "SKIPPED"
        folder_ready, folder_reason = self._is_folder_ready_for_move(folder)
        if not folder_ready:
            logger.info(f"【TMM转移助手】跳过未完成入库目录 [{folder.name}]：{folder_reason}")
            return "SKIPPED"

        if mode == "movie":
            target_root = Path(self._default_movie_path)
            category_name = ""
        else:
            tvshow_nfo = folder / "tvshow.nfo"
            if not tvshow_nfo.exists():
                for f in nfo_files:
                    if f.name.lower() == "tvshow.nfo": tvshow_nfo = f; break
            if not tvshow_nfo.exists(): return "SKIPPED"
            target_root = self._resolve_series_target_root(tvshow_nfo)
            category_name = target_root.name

        target_dir = target_root / folder.name
        if target_dir.exists(): return "SKIPPED"
            
        target_dir.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.move(str(folder), str(target_dir))
            logger.info(f"【TMM转移助手】✔ 成功入库: {target_dir.name}")
            if self._notify_enabled:
                self._send_item_notification(target_dir, mode, category_name)
            return "MOVED"
        except Exception as e:
            logger.error(f"【TMM转移助手】❌ 移动失败 [{folder.name}]: {str(e)}")
            return "ERROR"

    def _is_folder_ready_for_move(self, folder: Path) -> Tuple[bool, str]:
        latest_media_mtime = 0.0
        for file_path in folder.rglob("*"):
            if not file_path.is_file():
                continue
            if self._is_temp_download_file(file_path):
                return False, f"检测到未完成下载临时文件 {file_path.name}"
            if file_path.suffix.lower() in self.MEDIA_EXTENSIONS:
                latest_media_mtime = max(latest_media_mtime, file_path.stat().st_mtime)

        quiet_seconds = max(0, self._move_quiet_minutes * 60)
        if latest_media_mtime and quiet_seconds > 0:
            quiet_age = time.time() - latest_media_mtime
            if quiet_age < quiet_seconds:
                return False, f"媒体文件最近 {max(1, int(quiet_age // 60))} 分钟内仍有写入"
        return True, "目录已稳定"

    def _send_item_notification(self, target_dir: Path, mode: str, category: str):
        try:
            nfo_file = None
            if mode == "series":
                nfo_file = target_dir / "tvshow.nfo"
            else:
                nfo_files = list(target_dir.glob("*.nfo"))
                if nfo_files:
                    for f in nfo_files:
                        if f.name.lower() != "tvshow.nfo": nfo_file = f; break
                    if not nfo_file: nfo_file = nfo_files[0]
            if not nfo_file: return

            tree = ET.parse(nfo_file)
            root = tree.getroot()

            title = root.findtext("title") or target_dir.name
            year = root.findtext("year") or ""
            plot = self._truncate_wecom_overview(root.findtext("plot") or "暂无简介")

            rating = "0.0"
            ratings_node = root.find("ratings")
            if ratings_node is not None:
                for r in ratings_node.findall("rating"):
                    if r.get("name", "").lower() in ["tmdb", "themoviedb", "imdb"]:
                        val = r.find("value")
                        if val is not None and val.text: rating = val.text.strip(); break
            if rating == "0.0":
                rating_node = root.find(".//rating")
                if rating_node is not None:
                    val = rating_node.find("value")
                    if val is not None and val.text: rating = val.text.strip()
                    elif rating_node.text and rating_node.text.strip(): rating = rating_node.text.strip()

            image_url, poster_url = self._extract_notification_images(root)

            if rating in ["0.0", "0", ""] or not image_url or not poster_url:
                tmdb_id = None
                for uid in root.findall(".//uniqueid"):
                    if uid.get("type", "").lower() in ["tmdb", "themoviedb"]:
                        tmdb_id = uid.text; break
                if not tmdb_id:
                    tmdb_id = root.findtext(".//tmdbid") or root.findtext(".//tmdbId")
                
                if tmdb_id and str(tmdb_id).isdigit():
                    try:
                        mtype = MediaType.MOVIE if mode == "movie" else MediaType.TV
                        tmdb_info = TmdbChain().tmdb_info(tmdbid=int(tmdb_id), mtype=mtype)
                        if tmdb_info:
                            if rating in ["0.0", "0", ""] and tmdb_info.vote_average:
                                rating = str(tmdb_info.vote_average)
                            if not image_url:
                                image_url = tmdb_info.get_message_image()
                            if not poster_url and hasattr(tmdb_info, "get_poster_image"):
                                poster_url = tmdb_info.get_poster_image()
                    except Exception as e:
                        logger.error(f"【TMM转移助手】通过 TMDBID 提取补齐数据失败: {e}")

            try: rating = f"{float(rating):.1f}"
            except: rating = "0.0"

            # ========================
            # 2.0.4 优化：电影流派智能汉化
            # ========================
            if mode == "movie" and not category:
                genres = []
                for g in root.findall(".//genre"):
                    if g.text:
                        parts = [p.strip() for p in g.text.replace("|", "/").split("/") if p.strip()]
                        for p in parts:
                            genres.append(self.GENRE_MAPPING.get(p.lower(), p))
                
                unique_genres = []
                for g in genres:
                    if g not in unique_genres:
                        unique_genres.append(g)
                category = " / ".join(unique_genres)

            # ========================
            # 2.0.4 优化：不连续季数合并算法 (如 S01,S03-S05)
            # ========================
            season_str = ""
            if mode == "series":
                seasons = set()
                # 从文件夹名提取 (如 Season 1, Season 02)
                for p in target_dir.iterdir():
                    if p.is_dir():
                        m = re.search(r'(?:Season\s*|S)(\d+)', p.name, re.IGNORECASE)
                        if m: seasons.add(int(m.group(1)))
                # 从媒体文件名提取 (如 S01E01)
                for f in target_dir.rglob("*"):
                    if f.is_file() and f.suffix.lower() in self.MEDIA_EXTENSIONS:
                        m = re.search(r'[Ss](\d{1,4})[Ee]\d+', f.name)
                        if m: seasons.add(int(m.group(1)))

                if seasons:
                    sorted_s = sorted(list(seasons))
                    ranges = []
                    start = sorted_s[0]
                    prev = sorted_s[0]
                    
                    for s in sorted_s[1:]:
                        if s == prev + 1:
                            prev = s
                        else:
                            if start == prev:
                                ranges.append(f"S{start:02d}")
                            else:
                                ranges.append(f"S{start:02d}-S{prev:02d}")
                            start = s
                            prev = s
                    
                    # 收尾最后一个区间
                    if start == prev:
                        ranges.append(f"S{start:02d}")
                    else:
                        ranges.append(f"S{start:02d}-S{prev:02d}")
                        
                    season_str = " " + ",".join(ranges)

            file_count, total_bytes = 0, 0
            for f in target_dir.rglob("*"):
                if f.is_file():
                    total_bytes += f.stat().st_size
                    if f.suffix.lower() in self.MEDIA_EXTENSIONS: file_count += 1
            if file_count == 0:
                for f in target_dir.rglob("*"):
                    if f.is_file() and f.suffix.lower() not in {".nfo", ".jpg", ".png", ".srt", ".ass"}:
                        file_count += 1
            total_size = f"{total_bytes / (1024 ** 3):.2f} GB" if total_bytes > 1024**3 else f"{total_bytes / (1024 ** 2):.2f} MB"

            res_term = ""
            if "4k" in target_dir.name.lower() or "2160p" in target_dir.name.lower(): res_term = "4K"
            elif "1080p" in target_dir.name.lower(): res_term = "1080p"

            # 将季数追加在标题后方
            msg_title = f"《{title}{' (' + year + ')' if year else ''}》{season_str} 已入库 ✅"
            parts = [f"⭐️评分：{rating}", f"🎬类型：{'电影' if mode=='movie' else '剧集'}"]
            if category: parts.append(f"📁类别：{category}")
            if res_term: parts.append(f"📦质量：{res_term}")
            
            msg_text = (
                f"{' ｜ '.join(parts)}\n\n"
                f"📝简介：{plot}\n\n"
                f"📄共 {file_count} 个文件 ｜ 💾大小：{total_size}"
            )

            notify_image = image_url or poster_url or None
            self.post_message(title=msg_title, text=msg_text, image=notify_image)

        except Exception as e:
            logger.error(f"【TMM转移助手】发送入库通知异常 [{target_dir.name}]: {str(e)}")

    def _resolve_series_target_root(self, tvshow_nfo: Path) -> Path:
        values = []
        try:
            root = ET.parse(tvshow_nfo).getroot()
            for tag in ("country", "genre"):
                for node in root.findall(f".//{tag}"):
                    if node.text: values.extend([p.strip() for p in node.text.replace("|", "/").replace(",", "/").split("/") if p.strip()])
        except: pass
        
        normalized_values = [v.lower() for v in values]
        category_rules = [
            ("anime", ["动漫", "动画", "anime", "animation"]), ("shortdrama", ["短剧", "微短剧"]),
            ("documentary", ["纪录片", "documentary"]), ("variety", ["综艺", "真人秀"]),
            ("hktw", ["香港", "台湾", "港台"]), ("jpkr", ["日本", "韩国", "日韩"]),
            ("mainland", ["中国大陆", "中国", "大陆"]), ("western", ["美国", "英国", "欧美", "欧洲"]),
        ]
        for key, keywords in category_rules:
            if any(k.lower() in val for val in normalized_values for k in keywords):
                return Path(self._default_series_path) / self.SERIES_CATEGORIES[key]
        return Path(self._default_series_path) / self.SERIES_CATEGORIES["western"]

    def _safe_move_folder(self, src_dir: Path, dst_dir: Path) -> str:
        if dst_dir.exists(): return "SKIPPED_EXISTS"
        dst_dir.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.move(str(src_dir), str(dst_dir))
            logger.info(f"【TMM转移助手】✔ 成功移动: {dst_dir.name}")
            return "MOVED"
        except Exception as e:
            logger.error(f"【TMM转移助手】❌ 移动失败 [{src_dir.name}]: {str(e)}")
            return "ERROR"
