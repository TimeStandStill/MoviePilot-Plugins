"""Daily Emby update report plugin for MoviePilot V3."""

import json
import ssl
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from apscheduler.triggers.cron import CronTrigger

from app.sdk.logging import logger
from app.plugins import _PluginBase


class DailyMediaReport(_PluginBase):
    """Collect today's Emby additions and send one MoviePilot notification."""

    plugin_name = "今日影视更新播报"
    plugin_desc = "汇总订阅剧集今日更新和电影今日入库，并通过 MoviePilot 通知渠道发送一条图文播报。"
    plugin_version = "2.0.0"
    plugin_author = "QB"
    author_url = "https://github.com/TimeStandStill/MoviePilot-Plugins"
    plugin_icon = "Emby_A.png"
    plugin_order = 67

    SUBSCRIBED_LIBRARY = "订阅剧集"
    MOVIE_LIBRARY = "电影"
    DAILY_UPDATE_TYPE = "daily_media_update"
    NOTIFICATION_HANDLERS = {
        DAILY_UPDATE_TYPE: "_run_daily_media_update",
    }
    DEFAULT_HEADER_IMAGE = (
        "https://raw.githubusercontent.com/TimeStandStill/MoviePilot-Plugins/"
        "main/plugins.v3/dailymediareport/assets/daily-media-header.png"
    )
    GENRE_MAPPING = {
        "action": "动作", "adventure": "冒险", "animation": "动画", "anime": "动画",
        "comedy": "喜剧", "crime": "犯罪", "documentary": "纪录片", "drama": "剧情",
        "family": "家庭", "fantasy": "奇幻", "history": "历史", "horror": "恐怖",
        "mystery": "悬疑", "romance": "爱情", "science fiction": "科幻",
        "sci-fi": "科幻", "sci-fi & fantasy": "科幻", "science-fiction": "科幻",
        "thriller": "惊悚", "war": "战争", "western": "西部", "reality": "真人秀",
        "talk show": "脱口秀",
    }
    LOG_TAG = "【今日影视播报】"

    def __init__(self):
        super().__init__()
        self._enabled = False
        self._emby_url = ""
        self._emby_api_key = ""
        self._notify_time = "21:00"
        self._cron = "0 21 * * *"
        self._header_image_url = self.DEFAULT_HEADER_IMAGE
        self._verify_ssl = False
        self._once_per_day = True
        self._run_once = False
        self._last_result: Dict[str, Any] = {}

    def init_plugin(self, config: dict = None):
        config = config or {}
        self._emby_url = (config.get("emby_url") or "").strip().rstrip("/")
        self._emby_api_key = (config.get("emby_api_key") or "").strip()
        self._notify_time = self._normalize_time(config.get("notify_time") or "21:00")
        self._cron = self._time_to_cron(self._notify_time)
        self._header_image_url = (config.get("header_image_url") or self.DEFAULT_HEADER_IMAGE).strip()
        self._verify_ssl = bool(config.get("verify_ssl", False))
        self._once_per_day = bool(config.get("once_per_day", True))
        self._run_once = bool(config.get("run_once", False))
        self._enabled = bool(config.get("enabled") and self._emby_url and self._emby_api_key)
        logger.info(
            f"{self.LOG_TAG} 配置加载：启用={self._enabled}，通知类别=今日影视更新播报，"
            f"通知时间={self._notify_time}，每日去重={self._once_per_day}，证书校验={self._verify_ssl}"
        )

        if self._run_once and self._enabled:
            threading.Thread(target=self.run_report, kwargs={"force": True, "source": "保存后手动"}, daemon=True).start()
            self._run_once = False
            self._save_config()

    def get_state(self) -> bool:
        return self._enabled

    def _save_config(self) -> bool:
        return self.update_config({
            "enabled": self._enabled,
            "emby_url": self._emby_url,
            "emby_api_key": self._emby_api_key,
            "notify_time": self._notify_time,
            "header_image_url": self._header_image_url,
            "verify_ssl": self._verify_ssl,
            "once_per_day": self._once_per_day,
            "run_once": self._run_once,
        })

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [{
            "cmd": "plugin/DailyMediaReport/run",
            "method": "post",
            "text": "立即发送今日影视播报",
            "icon": "mdi-send",
            "color": "primary",
        }]

    def get_api(self) -> List[Dict[str, Any]]:
        return [{
            "path": "/run",
            "endpoint": self.api_run_report,
            "auth": "bear",
            "methods": ["POST"],
            "summary": "立即生成并发送今日影视更新播报",
        }]

    def get_service(self) -> List[Dict[str, Any]]:
        if not self.get_state() or not self._cron:
            return []
        try:
            return [{
                "id": "daily_media_report_job",
                "name": f"今日影视更新播报（每日 {self._notify_time}）",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.run_report,
                "kwargs": {"force": False, "source": "定时任务"},
            }]
        except Exception as err:
            logger.error(f"{self.LOG_TAG} Cron 配置无效 [{self._cron}]：{err}")
            return []

    def stop_service(self):
        """MoviePilot manages the scheduled job; this plugin owns no persistent worker."""

    def api_run_report(self) -> Dict[str, Any]:
        if not self.get_state():
            return {"code": 1, "msg": "请先启用插件并填写 Emby 地址与 API Key。"}
        threading.Thread(target=self.run_report, kwargs={"force": True, "source": "手动执行"}, daemon=True).start()
        return {"code": 0, "msg": "✅ 播报任务已在后台启动。"}

    def get_form(self) -> Tuple[Optional[List[dict]], Dict[str, Any]]:
        form = [{"component": "VForm", "content": [
            {"component": "VTabs", "props": {"model": "active_notification_tab", "color": "primary", "grow": True}, "content": [
                {"component": "VTab", "props": {"value": "daily_media_update"}, "text": "今日影视更新播报"},
                {"component": "VTab", "props": {"value": "reserved", "disabled": True}, "text": "更多通知类别（敬请期待）"},
            ]},
            {"component": "VWindow", "props": {"model": "active_notification_tab", "class": "mt-4"}, "content": [
                {"component": "VWindowItem", "props": {"value": "daily_media_update"}, "content": [
            {"component": "VRow", "content": [
                {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                    {"component": "VSwitch", "props": {"model": "enabled", "label": "启用今日影视播报", "color": "primary"}}
                ]},
                {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                    {"component": "VSwitch", "props": {"model": "once_per_day", "label": "同一天只自动发送一次", "color": "info"}}
                ]},
                {"component": "VCol", "props": {"cols": 12, "md": 4}, "content": [
                    {"component": "VSwitch", "props": {"model": "run_once", "label": "保存后立即发送一次", "color": "warning"}}
                ]},
            ]},
            {"component": "VRow", "content": [
                {"component": "VCol", "props": {"cols": 12, "md": 7}, "content": [
                    {"component": "VTextField", "props": {"model": "emby_url", "label": "Emby 地址", "placeholder": "https://emby.example.com"}}
                ]},
                {"component": "VCol", "props": {"cols": 12, "md": 5}, "content": [
                    {"component": "VTextField", "props": {"model": "emby_api_key", "label": "Emby API Key", "type": "password", "autocomplete": "new-password"}}
                ]},
            ]},
            {"component": "VRow", "content": [
                {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                    {"component": "VTextField", "props": {"model": "notify_time", "label": "每日通知时间", "placeholder": "21:00", "hint": "24 小时制 HH:MM；按 MoviePilot 服务器时区执行。", "persistent-hint": True}}
                ]},
                {"component": "VCol", "props": {"cols": 12, "md": 6}, "content": [
                    {"component": "VSwitch", "props": {"model": "verify_ssl", "label": "校验 Emby HTTPS 证书", "color": "info", "hint": "自签名证书可关闭；公网有效证书建议开启。", "persistent-hint": True}}
                ]},
            ]},
            {"component": "VRow", "content": [
                {"component": "VCol", "props": {"cols": 12}, "content": [
                    {"component": "VTextField", "props": {"model": "header_image_url", "label": "播报头图 URL", "placeholder": self.DEFAULT_HEADER_IMAGE, "hint": "需为通知渠道可访问的公开 HTTPS 图片。留空时使用插件默认头图。", "persistent-hint": True}}
                ]},
            ]},
            {"component": "VAlert", "props": {"type": "info", "variant": "tonal", "text": "当前类别“今日影视更新播报”直接调用 Emby API：订阅剧集按当天新入库 Episode 聚合，电影按当天 Movie.DateCreated 聚合。它通过 MoviePilot 的统一 post_message 发送一条图文通知，因此复用你已经配置的飞书机器人/通知渠道，不在插件内保存飞书凭据。所有请求、筛选、跳过与发送结果均带【今日影视播报】标签写入 MoviePilot 日志。"}},
                ]},
                {"component": "VWindowItem", "props": {"value": "reserved"}, "content": [
                    {"component": "VAlert", "props": {"type": "info", "variant": "tonal", "text": "此页签为后续发版预留；新版通知类别会在此处增加，不会影响已配置的今日影视更新播报。"}},
                ]},
            ]},
        ]}]
        model = {
            "active_notification_tab": self.DAILY_UPDATE_TYPE,
            "enabled": False,
            "emby_url": "",
            "emby_api_key": "",
            "notify_time": "21:00",
            "header_image_url": self.DEFAULT_HEADER_IMAGE,
            "verify_ssl": False,
            "once_per_day": True,
            "run_once": False,
        }
        return form, model

    def get_page(self) -> Optional[List[dict]]:
        summary = self._last_result.get("summary") or "尚未执行播报。"
        return [{"component": "VCard", "props": {"variant": "outlined", "class": "mb-4"}, "content": [
            {"component": "VCardText", "props": {"class": "pa-6 d-flex flex-column align-center"}, "content": [
                {"component": "VIcon", "props": {"icon": "mdi-television-play", "size": "64", "color": "primary", "class": "mb-4"}},
                {"component": "div", "props": {"class": "text-h6 mb-2"}, "text": "今日影视更新播报"},
                {"component": "div", "props": {"class": "text-body-2 text-medium-emphasis mb-6 text-center"}, "text": summary},
                {"component": "VBtn", "props": {"color": "primary", "variant": "elevated", "size": "large", "prepend-icon": "mdi-send"}, "text": "立即发送今日播报", "events": {"click": {"api": "plugin/DailyMediaReport/run", "method": "post"}}},
            ]},
        ]}]

    def run_report(self, force: bool = False, source: str = "定时任务") -> None:
        """Dispatch the enabled notification category; future releases add handlers here."""
        handler_name = self.NOTIFICATION_HANDLERS.get(self.DAILY_UPDATE_TYPE)
        handler = getattr(self, handler_name, None) if handler_name else None
        if not handler:
            logger.error(f"{self.LOG_TAG} 未注册通知类别处理器：{self.DAILY_UPDATE_TYPE}")
            return
        handler(force=force, source=source)

    def _run_daily_media_update(self, force: bool = False, source: str = "定时任务") -> None:
        """Current first notification category: daily subscribed-series and movie report."""
        if not self.get_state():
            logger.warning(f"{self.LOG_TAG} 插件未就绪，跳过{source}")
            return
        report_date = datetime.now().astimezone().date().isoformat()
        logger.info(f"{self.LOG_TAG} 开始{source}：日期={report_date}，类别=今日影视更新播报")
        if self._once_per_day and not force and self.get_data("last_report_date") == report_date:
            self._last_result = {"summary": f"{report_date} 已自动发送过，跳过重复播报。"}
            logger.info(f"{self.LOG_TAG} {report_date} 已发送过，跳过重复播报")
            return
        try:
            series, movies = self._collect_today_items(report_date)
            total = len(series) + len(movies)
            logger.info(f"{self.LOG_TAG} 今日筛选结果：剧集={len(series)}，电影={len(movies)}，合计={total}")
            if not total:
                self._last_result = {"summary": f"{report_date} 没有订阅剧集更新或电影入库。"}
                logger.info(f"{self.LOG_TAG} {report_date} 无需播报")
                return
            title, text = self._build_message(report_date, series, movies)
            logger.info(f"{self.LOG_TAG} 正在调用 MoviePilot 通知链路发送图文播报")
            self.post_message(title=title, text=text, image=self._header_image_url or None)
            self.save_data("last_report_date", report_date)
            self._last_result = {"summary": f"{report_date} 已发送：{len(series)} 部剧集更新，{len(movies)} 部电影入库。"}
            logger.info(f"{self.LOG_TAG} {self._last_result['summary']}")
        except Exception as err:
            self._last_result = {"summary": f"播报失败：{err}"}
            logger.error(f"{self.LOG_TAG} {source}失败：{err}")

    def _collect_today_items(self, report_date: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        libraries = self._api_get("/Library/VirtualFolders")
        ids = {str(item.get("Name")): str(item.get("ItemId")) for item in libraries if item.get("Name") in {self.SUBSCRIBED_LIBRARY, self.MOVIE_LIBRARY}}
        if self.SUBSCRIBED_LIBRARY not in ids or self.MOVIE_LIBRARY not in ids:
            missing = [name for name in (self.SUBSCRIBED_LIBRARY, self.MOVIE_LIBRARY) if name not in ids]
            raise RuntimeError(f"Emby 未找到媒体库：{'、'.join(missing)}")
        logger.info(f"{self.LOG_TAG} 已定位 Emby 媒体库：订阅剧集={ids[self.SUBSCRIBED_LIBRARY]}，电影={ids[self.MOVIE_LIBRARY]}")
        series = self._collect_series(ids[self.SUBSCRIBED_LIBRARY], report_date)
        movies = self._collect_movies(ids[self.MOVIE_LIBRARY], report_date)
        return series, movies

    def _collect_series(self, parent_id: str, report_date: str) -> List[Dict[str, Any]]:
        fields = "ProductionYear,DateCreated,CommunityRating,Genres,SeriesId,SeriesName,IndexNumber,ParentIndexNumber"
        raw = self._list_items(parent_id, "Series,Episode", fields)
        logger.info(f"{self.LOG_TAG} Emby 订阅剧集库读取完成：原始项目数={len(raw)}")
        series_by_id = {str(item.get("Id")): item for item in raw if item.get("Type") == "Series"}
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for episode in raw:
            if episode.get("Type") != "Episode" or not self._is_report_date(episode.get("DateCreated"), report_date):
                continue
            series_id = str(episode.get("SeriesId") or "")
            if series_id:
                grouped.setdefault(series_id, []).append(episode)
        result = []
        for series_id, episodes in grouped.items():
            info = series_by_id.get(series_id, {})
            codes = sorted({self._episode_code(item) for item in episodes if self._episode_code(item)})
            seasons = sorted({code[:3] for code in codes})
            result.append({
                "title": info.get("Name") or episodes[0].get("SeriesName") or "未命名剧集",
                "season": "、".join(seasons) or "—",
                "episodes": "、".join(codes) or "—",
                "rating": info.get("CommunityRating"),
                "category": self._normalize_genres(info.get("Genres")),
            })
        return sorted(result, key=lambda item: item["title"])

    def _collect_movies(self, parent_id: str, report_date: str) -> List[Dict[str, Any]]:
        fields = "ProductionYear,DateCreated,CommunityRating,Genres"
        raw = self._list_items(parent_id, "Movie", fields)
        logger.info(f"{self.LOG_TAG} Emby 电影库读取完成：原始项目数={len(raw)}")
        result = [{
            "title": item.get("Name") or "未命名电影",
            "year": item.get("ProductionYear"),
            "rating": item.get("CommunityRating"),
            "category": self._normalize_genres(item.get("Genres")),
        } for item in raw if self._is_report_date(item.get("DateCreated"), report_date)]
        return sorted(result, key=lambda item: (item.get("year") or 0, item["title"]), reverse=True)

    def _list_items(self, parent_id: str, item_types: str, fields: str) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        start = 0
        while True:
            page = self._api_get("/Items", {
                "ParentId": parent_id,
                "Recursive": "true",
                "IncludeItemTypes": item_types,
                "Fields": fields,
                "SortBy": "SortName",
                "SortOrder": "Ascending",
                "StartIndex": start,
                "Limit": 1000,
            })
            items = page.get("Items") or []
            logger.debug(f"{self.LOG_TAG} Emby 分页读取：类型={item_types}，偏移={start}，本页={len(items)}")
            result.extend(items)
            start += len(items)
            if not items or start >= int(page.get("TotalRecordCount", len(result))):
                return result

    def _api_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        query = dict(params or {})
        query["api_key"] = self._emby_api_key
        url = f"{self._emby_url}{path}?{urllib.parse.urlencode(query)}"
        context = None if self._verify_ssl else ssl._create_unverified_context()
        last_error = None
        for attempt in range(3):
            try:
                request = urllib.request.Request(url, headers={"Accept": "application/json"})
                with urllib.request.urlopen(request, context=context, timeout=30) as response:
                    return json.loads(response.read().decode("utf-8"))
            except Exception as err:
                last_error = err
                time.sleep(attempt + 1)
        raise RuntimeError(f"Emby 请求失败：{last_error}")

    @staticmethod
    def _normalize_time(value: Any) -> str:
        try:
            parsed = datetime.strptime(str(value).strip(), "%H:%M")
            return parsed.strftime("%H:%M")
        except (TypeError, ValueError):
            return "21:00"

    @staticmethod
    def _time_to_cron(value: str) -> str:
        hour, minute = DailyMediaReport._normalize_time(value).split(":")
        return f"{int(minute)} {int(hour)} * * *"

    @staticmethod
    def _episode_code(item: Dict[str, Any]) -> str:
        season = item.get("ParentIndexNumber")
        episode = item.get("IndexNumber")
        if season is None or episode is None:
            return ""
        return f"S{int(season):02d}E{int(episode):02d}"

    @staticmethod
    def _is_report_date(value: Any, report_date: str) -> bool:
        if not value:
            return False
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone().date().isoformat() == report_date
        except ValueError:
            return str(value)[:10] == report_date

    def _normalize_genres(self, genres: Any) -> str:
        normalized = []
        for genre in genres or []:
            value = str(genre).strip()
            mapped = self.GENRE_MAPPING.get(value.lower(), value)
            if mapped and mapped not in normalized:
                normalized.append(mapped)
        return "、".join(normalized) or "—"

    @staticmethod
    def _rating(value: Any) -> str:
        if value in (None, ""):
            return ""
        return str(value).rstrip("0").rstrip(".") if isinstance(value, float) else str(value)

    def _build_message(self, report_date: str, series: List[Dict[str, Any]], movies: List[Dict[str, Any]]) -> Tuple[str, str]:
        lines = [f"✨ 今日共更新 {len(series) + len(movies)} 部作品", "━━━━━━━━━━━━━━━━━━"]
        if series:
            lines.append("🎬 剧集更新")
            for item in series:
                details = f"   🚀 进度：{item['episodes']}"
                if self._rating(item.get("rating")):
                    details += f" ｜ ⭐ {self._rating(item['rating'])}"
                if item.get("category") and item["category"] != "—":
                    details += f" ｜ 🏷️ {item['category']}"
                lines.extend((f"• {item['title']} {item['season']}", details))
            lines.append("")
        if movies:
            lines.append("🍿 新片入库")
            for item in movies:
                details = f"• {item['title']} ({item.get('year') or '—'})"
                if self._rating(item.get("rating")):
                    details += f" ｜ ⭐ {self._rating(item['rating'])}"
                if item.get("category") and item["category"] != "—":
                    details += f" ｜ 🏷️ {item['category']}"
                lines.append(details)
        return f"📺 今日影视更新播报 ｜ {report_date}", "\n".join(lines)
