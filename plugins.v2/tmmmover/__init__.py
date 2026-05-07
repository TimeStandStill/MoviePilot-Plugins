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
    plugin_version = "2.0.6"
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

            media_exts = {".mp4", ".mkv", ".ts", ".avi", ".rmvb", ".wmv", ".iso", ".m2ts"}

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
                    if f.is_file() and f.suffix.lower() in media_exts:
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
                    if f.suffix.lower() in media_exts: file_count += 1
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
