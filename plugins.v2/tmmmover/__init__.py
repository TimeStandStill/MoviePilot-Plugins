import shutil
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from apscheduler.triggers.cron import CronTrigger

from app import schemas
from app.log import logger
from app.plugins import _PluginBase


class TMMMover(_PluginBase):
    """
    TMM 元数据智能转移助手
    - 定时扫描来源目录一级子目录
    - 识别 movie.nfo / tvshow.nfo 判断媒体类型
    - 剧集根据 country/genre 路由到细分目录
    - 使用 shutil.move 进行跨挂载点安全迁移
    """

    plugin_name = "TMM 元数据转移助手"
    plugin_desc = "根据 TMM NFO 元数据自动分拣并跨挂载点迁移媒体目录"
    plugin_version = "1.0.6"
    plugin_author = "MoviePilot"
    author_url = "https://github.com/jxxghp/MoviePilot"
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot/main/app.ico"
    plugin_order = 66

    # 固定剧集分类目录
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

    def init_plugin(self, config: dict = None):
        """
        生效配置信息
        """
        config = config or {}

        self._source_movie_path = (config.get("source_movie_path") or "").strip()
        self._source_series_path = (config.get("source_series_path") or "").strip()
        self._default_movie_path = (config.get("default_movie_path") or "").strip()
        self._default_series_path = (config.get("default_series_path") or "").strip()
        self._cron = (config.get("cron") or "").strip()

        movie_ready = bool(self._source_movie_path and self._default_movie_path)
        series_ready = bool(self._source_series_path and self._default_series_path)
        self._enabled = bool(movie_ready or series_ready)

        logger.info(
            f"{self.plugin_name} 配置已加载: movie_ready={movie_ready}, series_ready={series_ready}, cron={self._cron}"
        )

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        在插件列表页的卡片上提供快捷运行按钮
        """
        return [{
            "cmd": "/api/v1/plugin/TMMMover/run",
            "method": "post",
            "text": "立即运行",
            "icon": "mdi-play",
            "color": "primary"
        }]

    def get_api(self) -> List[Dict[str, Any]]:
        """
        提供外部与前端命令调用的 API 接口
        """
        return [
            {
                "path": "/run",
                "endpoint": self.api_run_once,
                "methods": ["POST"],
                "summary": "手动触发一次 TMMMover 任务"
            }
        ]

    def get_form(self) -> Tuple[Optional[List[dict]], Dict[str, Any]]:
        """
        插件配置表单（Vuetify）
        """
        form = [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VPathField",
                                        "props": {
                                            "model": "source_movie_path",
                                            "label": "电影来源监控目录",
                                            "placeholder": "/media/source/Movies",
                                            "hint": "包含 movie.nfo 的一级子目录",
                                            "persistent-hint": True
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VPathField",
                                        "props": {
                                            "model": "source_series_path",
                                            "label": "剧集来源监控目录",
                                            "placeholder": "/media/source/Series",
                                            "hint": "包含 tvshow.nfo 的一级子目录",
                                            "persistent-hint": True
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VPathField",
                                        "props": {
                                            "model": "default_movie_path",
                                            "label": "目标电影存放目录",
                                            "placeholder": "/media/movies"
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VPathField",
                                        "props": {
                                            "model": "default_series_path",
                                            "label": "目标剧集存放根目录",
                                            "placeholder": "/media/series",
                                            "hint": "插件会在该目录下自动创建分类子目录（如：动漫、大陆剧集等）",
                                            "persistent-hint": True
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VCronField",
                                        "props": {
                                            "model": "cron",
                                            "label": "定时执行 Cron 表达式",
                                            "placeholder": "0 */6 * * *",
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]

        model = {
            "source_movie_path": "",
            "source_series_path": "",
            "default_movie_path": "",
            "default_series_path": "",
            "cron": "0 */6 * * *"
        }
        return form, model

    def get_page(self) -> Optional[List[dict]]:
        """
        获取插件详情页面，展示手动运行按钮
        """
        return [
            {
                "component": "VCard",
                "props": {"variant": "outlined", "class": "mb-4"},
                "content": [
                    {
                        "component": "VCardText",
                        "props": {"class": "pa-6 d-flex flex-column align-center"},
                        "content": [
                            {
                                "component": "VIcon",
                                "props": {
                                    "icon": "mdi-folder-move", 
                                    "size": "64", 
                                    "color": "primary", 
                                    "class": "mb-4"
                                }
                            },
                            {
                                "component": "div",
                                "props": {"class": "text-h6 mb-2"},
                                "text": "手动执行迁移任务",
                            },
                            {
                                "component": "div",
                                "props": {"class": "text-body-2 text-medium-emphasis mb-6 text-center"},
                                "text": "点击下方按钮，立即扫描来源目录并执行 TMM 元数据分类与跨挂载点迁移操作。",
                            },
                            {
                                "component": "VBtn",
                                "props": {
                                    "color": "primary",
                                    "variant": "elevated",
                                    "size": "large",
                                    "prepend-icon": "mdi-rocket-launch",
                                },
                                "text": "立即运行",
                                "events": {
                                    "click": {
                                        "api": "plugin/TMMMover/run",
                                        "method": "post",
                                    },
                                },
                            },
                        ],
                    },
                ],
            }
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册定时任务
        """
        if not self.get_state() or not self._cron:
            return []

        try:
            trigger = CronTrigger.from_crontab(self._cron)
        except Exception as e:
            logger.error(f"{self.plugin_name} cron 非法({self._cron})：{str(e)}")
            return []

        return [
            {
                "id": "scan_move_job",
                "name": "TMM 目录转移任务",
                "trigger": trigger,
                "func": self.run_once
            }
        ]

    def stop_service(self):
        pass

    def api_run_once(self) -> schemas.Response:
        """
        供前端页面调用的手动触发接口
        """
        try:
            result = self.run_once()
            return schemas.Response(success=True, message=result)
        except Exception as e:
            logger.error(f"{self.plugin_name} 执行异常: {str(e)}")
            return schemas.Response(success=False, message=str(e))

    def run_once(self) -> str:
        """
        核心执行逻辑
        """
        if not self.get_state():
            msg = "插件未完全配置，任务中止"
            logger.warning(f"{self.plugin_name}: {msg}")
            return msg

        logger.info(f"{self.plugin_name}：开始执行扫描任务")
        movie_moved, movie_skipped, movie_err = self._scan_source_dir(self._source_movie_path, "movie")
        series_moved, series_skipped, series_err = self._scan_source_dir(self._source_series_path, "series")

        total_moved = movie_moved + series_moved
        total_skipped = movie_skipped + series_skipped
        total_err = movie_err + series_err
        
        summary = f"任务完成！成功转移: {total_moved}，跳过: {total_skipped}，失败: {total_err}。详情请查看系统日志。"
        logger.info(f"{self.plugin_name}: {summary}")
        return summary

    def _scan_source_dir(self, source_path: str, mode: str) -> Tuple[int, int, int]:
        if not source_path:
            return 0, 0, 0

        source_dir = Path(source_path)
        if not source_dir.exists() or not source_dir.is_dir():
            logger.warning(f"{self.plugin_name} 来源目录无效，跳过: {source_dir}")
            return 0, 0, 0

        moved, skipped, err = 0, 0, 0

        for child in source_dir.iterdir():
            if not child.is_dir() or self._is_deleted_by_tmm_dir(child):
                continue
            try:
                if self._process_one_folder(child, mode):
                    moved += 1
                else:
                    skipped += 1
            except Exception as e:
                err += 1
                logger.error(f"{self.plugin_name} 处理目录失败 {child}: {str(e)}")
                
        return moved, skipped, err

    def _is_deleted_by_tmm_dir(self, folder: Path) -> bool:
        name = folder.name.strip()
        return name == ".deletedByTMM" or name.endswith(".deletedByTMM")

    def _process_one_folder(self, folder: Path, mode: str) -> bool:
        movie_nfo = folder / "movie.nfo"
        tvshow_nfo = folder / "tvshow.nfo"

        # 解决部分文件系统下的文件名大小写兼容问题
        if not movie_nfo.exists() and not tvshow_nfo.exists():
            for f in folder.iterdir():
                if f.is_file():
                    if f.name.lower() == "movie.nfo":
                        movie_nfo = f
                    elif f.name.lower() == "tvshow.nfo":
                        tvshow_nfo = f

        if mode == "movie":
            if not movie_nfo.exists():
                return False
            target_root = Path(self._default_movie_path)
        else:
            if not tvshow_nfo.exists():
                return False
            target_root = self._resolve_series_target_root(tvshow_nfo)

        target_dir = target_root / folder.name
        return self._safe_move_folder(folder, target_dir)

    def _resolve_series_target_root(self, tvshow_nfo: Path) -> Path:
        meta_values = self._extract_tvshow_meta(tvshow_nfo)
        category_name = self._resolve_series_category_name(meta_values)
        return Path(self._default_series_path) / category_name

    def _resolve_series_category_name(self, meta_values: List[str]) -> str:
        normalized_values = [value.lower() for value in meta_values]
        category_rules = [
            ("anime", ["动漫", "动画", "anime", "animation"]),
            ("shortdrama", ["短剧", "微短剧", "短片"]),
            ("documentary", ["纪录片", "纪录", "documentary"]),
            ("variety", ["综艺", "真人秀", "脱口秀", "variety", "reality"]),
            ("hktw", ["香港", "台湾", "港台", "港剧", "台剧"]),
            ("jpkr", ["日本", "韩国", "日韩", "日剧", "韩剧"]),
            ("mainland", ["中国大陆", "中国", "大陆", "内地", "国产", "华语"]),
            ("western", ["美国", "英国", "欧美", "欧洲", "加拿大", "澳大利亚"]),
        ]

        for category_key, keywords in category_rules:
            if any(keyword.lower() in val for val in normalized_values for keyword in keywords):
                return self.SERIES_CATEGORIES[category_key]
                
        return self.SERIES_CATEGORIES["mainland"]

    def _extract_tvshow_meta(self, tvshow_nfo: Path) -> List[str]:
        """
        提取 NFO 中的流派和国家信息
        """
        values: List[str] = []
        try:
            tree = ET.parse(tvshow_nfo)
            root = tree.getroot()

            for tag in ("country", "genre"):
                for node in root.findall(f".//{tag}"):
                    text = (node.text or "").strip()
                    if not text:
                        continue
                    parts = [
                        p.strip() for p in text.replace("|", "/").replace(",", "/").replace("，", "/").split("/")
                        if p.strip()
                    ]
                    values.extend(parts)
        except Exception as e:
            logger.error(f"解析 NFO 失败 {tvshow_nfo}: {str(e)}")
            return []
            
        return self._deduplicate(values)

    def _deduplicate(self, values: List[str]) -> List[str]:
        uniq = []
        seen = set()
        for value in values:
            if value not in seen:
                seen.add(value)
                uniq.append(value)
        return uniq

    def _safe_move_folder(self, src_dir: Path, dst_dir: Path) -> bool:
        if dst_dir.exists():
            logger.info(f"{self.plugin_name} 目标目录已存在，跳过覆盖: {dst_dir}")
            return False

        dst_dir.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.move(str(src_dir), str(dst_dir))
            logger.info(f"{self.plugin_name} 成功移动: {src_dir.name} -> {dst_dir.parent.name}")
            return True
        except Exception as e:
            logger.error(f"{self.plugin_name} 移动失败 {src_dir.name}: {str(e)}")
            if dst_dir.exists() and src_dir.exists():
                 logger.error(f"发生不完整迁移，请手动检查: {dst_dir}")
            return False
