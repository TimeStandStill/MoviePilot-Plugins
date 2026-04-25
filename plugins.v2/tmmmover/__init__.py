import re
import shutil
import threading
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from apscheduler.triggers.cron import CronTrigger

from app.log import logger
from app.plugins import _PluginBase
from app.chain.tmdb import TmdbChain
from app.schemas.types import MediaType


class TMMMover(_PluginBase):
    """
    TMM 元数据智能转移助手
    """

    plugin_name = "TMM 元数据转移助手"
    plugin_desc = (
        "根据 TMM NFO 自动分拣迁移，并精准提取 TMDB 数据模拟原生图文入库通知"
    )
    plugin_version = "2.0.3"
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

    def __init__(self):
        super().__init__()
        self._enabled: bool = False
        self._source_movie_path: str = ""
        self._source_series_path: str = ""
        self._default_movie_path: str = ""
        self._default_series_path: str = ""
        self._cron: str = ""
        self._notify_enabled: bool = False

    def init_plugin(self, config: dict = None):
        config = config or {}
        self._source_movie_path = (config.get("source_movie_path") or "").strip()
        self._source_series_path = (config.get("source_series_path") or "").strip()
        self._default_movie_path = (config.get("default_movie_path") or "").strip()
        self._default_series_path = (config.get("default_series_path") or "").strip()
        self._cron = (config.get("cron") or "").strip()
        self._notify_enabled = config.get("notify_enabled", False)

        movie_ready = bool(self._source_movie_path and self._default_movie_path)
        series_ready = bool(self._source_series_path and self._default_series_path)
        self._enabled = bool(movie_ready or series_ready)

        logger.info(f"【TMM转移助手】配置加载: 启用={self._enabled}, 通知={self._notify_enabled}")

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [{"cmd": "plugin/TMMMover/run", "method": "post", "text": "立即运行", "icon": "mdi-play", "color": "primary"}]

    def get_api(self) -> List[Dict[str, Any]]:
        return [{"path": "/run", "endpoint": self.api_run_once, "auth": "bear", "methods": ["POST"], "summary": "手动触发 TMM 转移任务"}]

    def get_form(self) -> Tuple[Optional[List[dict]], Dict[str, Any]]:
        form = [{"component": "VForm", "content": [{"component": "VRow", "content": [
            {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VPathField", "props": {"model": "source_movie_path", "label": "电影来源目录", "placeholder": "/media/source/Movies", "persistent-hint": True}}]},
            {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VPathField", "props": {"model": "source_series_path", "label": "剧集来源目录", "placeholder": "/media/source/Series", "persistent-hint": True}}]},
            {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VPathField", "props": {"model": "default_movie_path", "label": "目标电影存放目录", "placeholder": "/media/movies"}}]},
            {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VPathField", "props": {"model": "default_series_path", "label": "目标剧集存放根目录", "placeholder": "/media/series", "persistent-hint": True}}]},
            {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VCronField", "props": {"model": "cron", "label": "定时执行 Cron 表达式", "placeholder": "0 * * * *"}}]},
            {"component": "VCol", "props": {"cols": 12}, "content": [{"component": "VSwitch", "props": {"model": "notify_enabled", "label": "启用精美入库通知", "color": "primary", "persistent-hint": True}}]},
        ]}]}]
        model = {"source_movie_path": "", "source_series_path": "", "default_movie_path": "", "default_series_path": "", "cron": "0 * * * *", "notify_enabled": False}
        return form, model

    def get_page(self) -> Optional[List[dict]]:
        return [{"component": "VCard", "props": {"variant": "outlined", "class": "mb-4"}, "content": [{"component": "VCardText", "props": {"class": "pa-6 d-flex flex-column align-center"}, "content": [
            {"component": "VIcon", "props": {"icon": "mdi-folder-move", "size": "64", "color": "primary", "class": "mb-4"}},
            {"component": "div", "props": {"class": "text-h6 mb-2"}, "text": "手动执行迁移任务"},
            {"component": "div", "props": {"class": "text-body-2 text-medium-emphasis mb-6 text-center"}, "text": "严格遵照 NFO 数据执行迁移与分类。静默处理跳过项，仅对成功移入库的媒体发送独立通知。"},
            {"component": "VBtn", "props": {"color": "primary", "variant": "elevated", "size": "large", "prepend-icon": "mdi-rocket-launch"}, "text": "立即运行", "events": {"click": {"api": "plugin/TMMMover/run", "method": "post"}}},
        ]}]}]

    def get_service(self) -> List[Dict[str, Any]]:
        if not self.get_state() or not self._cron: return []
        try: trigger = CronTrigger.from_crontab(self._cron)
        except: return []
        return [{"id": "scan_move_job", "name": "TMM 目录转移任务", "trigger": trigger, "func": self.run_once}]

    def stop_service(self):
        pass

    def api_run_once(self) -> Dict[str, Any]:
        threading.Thread(target=self.run_once, daemon=True).start()
        return {"code": 0, "msg": "✅ 任务已在后台启动！"}

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

    def _send_item_notification(self, target_dir: Path, mode: str, category: str):
        """
        NFO 本地解析优先，仅对缺失的公网图源与评分进行无脑按 ID 提取
        """
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

            # 1. 纯净提取 NFO 基础信息
            title = root.findtext("title") or target_dir.name
            year = root.findtext("year") or ""
            plot = root.findtext("plot") or "暂无简介"
            if len(plot) > 150: plot = plot[:150] + "..."

            # 2. 竭尽全力从 NFO 中翻找评分
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

            # 3. 寻找公网 HTTP 海报链接 (企微必须)
            image_url = ""
            for thumb in root.findall(".//thumb"):
                if thumb.text and thumb.text.startswith("http"):
                    if thumb.get("aspect") == "poster":
                        image_url = thumb.text.strip(); break
                    elif not image_url:
                        image_url = thumb.text.strip()

            # 4. 【轻量级补齐】如果没网图或者没评分，且有 TMDB ID，直接查 ID
            if rating in ["0.0", "0", ""] or not image_url:
                tmdb_id = None
                for uid in root.findall(".//uniqueid"):
                    if uid.get("type", "").lower() in ["tmdb", "themoviedb"]:
                        tmdb_id = uid.text; break
                if not tmdb_id:
                    tmdb_id = root.findtext(".//tmdbid") or root.findtext(".//tmdbId")
                
                if tmdb_id and str(tmdb_id).isdigit():
                    try:
                        mtype = MediaType.MOVIE if mode == "movie" else MediaType.TV
                        # 直接用 ID 查询，跳过所有 MP 的目录/正则/洗版识别链
                        tmdb_info = TmdbChain().tmdb_info(tmdbid=int(tmdb_id), mtype=mtype)
                        if tmdb_info:
                            if rating in ["0.0", "0", ""] and tmdb_info.vote_average:
                                rating = str(tmdb_info.vote_average)
                            if not image_url:
                                image_url = tmdb_info.get_message_image()
                    except Exception as e:
                        logger.error(f"【TMM转移助手】通过 TMDBID 提取补齐数据失败: {e}")

            try: rating = f"{float(rating):.1f}"
            except: rating = "0.0"

            if mode == "movie" and not category:
                category = root.findtext("genre") or ""

            # 5. 统计文件数和大小
            media_exts = {".mp4", ".mkv", ".ts", ".avi", ".rmvb", ".wmv", ".iso", ".m2ts"}
            file_count, total_bytes = 0, 0
            for f in target_dir.rglob("*"):
                if f.is_file():
                    total_bytes += f.stat().st_size
                    if f.suffix.lower() in media_exts: file_count += 1
            if file_count == 0:
                for f in target_dir.rglob("*"):
                    if f.is_file() and f.suffix.lower() not in {".nfo", ".jpg", ".png", ".srt", ".ass"}:
                        file_count += 1
            total_size = f"{total_bytes / (1024 ** 3):.2f} GB" if total_bytes > 1024**3 else f"{total_bytes / (1024 ** 2):.2f} MB"

            res_term = ""
            if "4k" in target_dir.name.lower() or "2160p" in target_dir.name.lower(): res_term = "4K"
            elif "1080p" in target_dir.name.lower(): res_term = "1080p"

            # 6. 纯正原生排版组装
            msg_title = f"《{title}{' (' + year + ')' if year else ''}》 已入库 ✅"
            parts = [f"⭐️评分：{rating}", f"🎬类型：{'电影' if mode=='movie' else '剧集'}"]
            if category: parts.append(f"📁类别：{category}")
            if res_term: parts.append(f"📦质量：{res_term}")
            
            msg_text = (
                f"{' ｜ '.join(parts)}\n\n"
                f"📝简介：{plot}\n\n"
                f"📄共 {file_count} 个文件 ｜ 💾大小：{total_size}"
            )

            # 空图片坚决不传，防止企微卡片变白板报错
            self.post_message(title=msg_title, text=msg_text, image=image_url if image_url else None)

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
