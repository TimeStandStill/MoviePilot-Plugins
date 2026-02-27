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

    plugin_name = "TMM 元数据智能转移助手"
    plugin_desc = "根据 TMM NFO 元数据自动分拣并跨挂载点安全迁移媒体目录"
    plugin_version = "1.0.4"
    plugin_author = "MoviePilot"
    author_url = "https://github.com/jxxghp/MoviePilot"
    plugin_icon = "https://raw.githubusercontent.com/jxxghp/MoviePilot/main/app.ico"
    plugin_order = 66
    # 固定剧集分类目录（按你的目录结构）
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
            f"{self.plugin_name} 配置已加载: source_movie={self._source_movie_path}, "
            f"source_series={self._source_series_path}, movie={self._default_movie_path}, "
            f"series={self._default_series_path}, cron={self._cron}"
        )

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """
        提供手动触发接口
        """
        return [
            {
                "path": "/run",
                "endpoint": self.api_run_once,
                "methods": ["POST"],
                "summary": "手动触发一次 TMMMover 任务",
                "description": "立即扫描来源目录并执行迁移任务"
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
                                            "label": "电影来源监控目录（Movies Source Path）",
                                            "placeholder": "/media/source/Movies",
                                            "hint": "仅遍历该目录下一级子目录，自动忽略 .deletedByTMM",
                                            "persistent-hint": True,
                                            "storage": "local"
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
                                            "label": "剧集来源监控目录（Series Source Path）",
                                            "placeholder": "/media/source/Series",
                                            "hint": "仅遍历该目录下一级子目录，自动忽略 .deletedByTMM",
                                            "persistent-hint": True,
                                            "storage": "local"
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
                                            "label": "默认电影目录（Default Movie Path）",
                                            "placeholder": "/media/movies",
                                            "storage": "local"
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
                                            "label": "默认剧集目录（Default Series Path）",
                                            "placeholder": "/media/series",
                                            "storage": "local"
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
                                            "hint": "标准 crontab 格式（分 时 日 月 周）",
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
                                        "component": "VBtn",
                                        "props": {
                                            "text": "立即运行一次",
                                            "color": "primary",
                                            "variant": "flat",
                                            "prependIcon": "mdi-play-circle",
                                            "onClick": "async function(){ try { const resp = await fetch('/api/v1/plugin/TMMMover/run', { method: 'POST', credentials: 'include' }); const data = await resp.json(); alert(data.message || '任务已触发'); } catch(e) { alert('触发失败，请查看控制台日志'); console.error(e); } }"
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
        插件详情页不展示内容。
        日志统一通过系统日志查看（插件外部“查看日志”）。
        """
        return None

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册定时任务
        """
        if not self.get_state():
            return []
        if not self._cron:
            logger.warning(f"{self.plugin_name} 未配置 cron，跳过任务注册")
            return []

        try:
            trigger = CronTrigger.from_crontab(self._cron)
        except Exception as e:
            logger.error(f"{self.plugin_name} cron 非法({self._cron})：{str(e)}")
            return []

        return [
            {
                "id": "scan_move_job",
                "name": "TMM 元数据目录转移任务",
                "trigger": trigger,
                "func": self.run_once
            }
        ]

    def stop_service(self):
        """
        无额外后台线程，保留空实现
        """
        pass

    def api_run_once(self) -> schemas.Response:
        """
        手动触发一次任务
        """
        result = self.run_once()
        return schemas.Response(success=True, message=result, data={})

    def run_once(self) -> str:
        """
        核心任务入口：扫描、分类、迁移、汇总通知
        """
        if not self.get_state():
            message = "插件未启用或关键配置缺失，任务未执行"
            logger.warning(f"{self.plugin_name}：{message}")
            return message

        logger.info(f"{self.plugin_name}：开始执行扫描任务")
        moved_count = 0
        skipped_count = 0
        error_count = 0

        movie_moved, movie_skipped, movie_error = self._scan_source_dir(
            source_path=self._source_movie_path,
            mode="movie"
        )
        series_moved, series_skipped, series_error = self._scan_source_dir(
            source_path=self._source_series_path,
            mode="series"
        )

        moved_count += movie_moved + series_moved
        skipped_count += movie_skipped + series_skipped
        error_count += movie_error + series_error

        summary = f"任务完成：成功移动 {moved_count} 个，跳过 {skipped_count} 个，失败 {error_count} 个"
        logger.info(f"{self.plugin_name}：{summary}")
        self.systemmessage.put(message=summary, role="system", title=self.plugin_name)
        return summary

    def _scan_source_dir(self, source_path: str, mode: str) -> Tuple[int, int, int]:
        """
        扫描指定来源目录（Movies 或 Series），只遍历一级子目录
        """
        if not source_path:
            return 0, 0, 0

        source_dir = Path(source_path)
        if not source_dir.exists() or not source_dir.is_dir():
            logger.warning(f"{self.plugin_name} 来源目录不存在，跳过：{source_dir}")
            return 0, 0, 0

        moved_count = 0
        skipped_count = 0
        error_count = 0

        for child in source_dir.iterdir():
            if not child.is_dir():
                continue
            # tmm 产生的回收目录，直接忽略
            if self._is_deleted_by_tmm_dir(child):
                logger.info(f"{self.plugin_name}：忽略目录 {child}")
                continue
            try:
                if self._process_one_folder(child, mode):
                    moved_count += 1
                else:
                    skipped_count += 1
            except Exception as e:
                error_count += 1
                logger.error(f"{self.plugin_name} 处理目录失败：{child}，错误：{str(e)}")
        return moved_count, skipped_count, error_count

    @staticmethod
    def _is_deleted_by_tmm_dir(folder: Path) -> bool:
        """
        忽略 TMM 删除缓存目录 .deletedByTMM（兼容部分后缀命名）
        """
        name = folder.name.strip()
        return name == ".deletedByTMM" or name.endswith(".deletedByTMM")

    def _process_one_folder(self, folder: Path, mode: str) -> bool:
        """
        处理单个一级目录
        """
        movie_nfo = folder / "movie.nfo"
        tvshow_nfo = folder / "tvshow.nfo"

        # 兼容大小写文件名
        if not movie_nfo.exists() and not tvshow_nfo.exists():
            file_map = {f.name.lower(): f for f in folder.iterdir() if f.is_file()}
            movie_nfo = file_map.get("movie.nfo", movie_nfo)
            tvshow_nfo = file_map.get("tvshow.nfo", tvshow_nfo)

        if mode == "movie":
            if not movie_nfo.exists():
                return False
            if not self._default_movie_path:
                logger.warning(f"{self.plugin_name} 未配置默认电影目录，跳过：{folder}")
                return False
            target_root = Path(self._default_movie_path)
        else:
            if not tvshow_nfo.exists():
                return False
            if not self._default_series_path:
                logger.warning(f"{self.plugin_name} 未配置默认剧集目录，跳过：{folder}")
                return False
            target_root = self._resolve_series_target_root(tvshow_nfo)

        target_dir = target_root / folder.name
        return self._safe_move_folder(folder, target_dir)

    def _resolve_series_target_root(self, tvshow_nfo: Path) -> Path:
        """
        剧集路由（固定规则，不走页面配置）：
        1. 从 tvshow.nfo 提取 country / genre
        2. 自动映射到固定分类目录
        3. 未命中时回落到默认剧集目录/大陆剧集
        """
        meta_values = self._extract_tvshow_meta(tvshow_nfo)
        category_name = self._resolve_series_category_name(meta_values)
        return Path(self._default_series_path) / category_name

    def _resolve_series_category_name(self, meta_values: List[str]) -> str:
        """
        根据内置分类规则匹配目标目录：
        大陆剧集 / 动漫 / 短剧 / 港台剧集 / 纪录片 / 欧美剧集 / 日韩剧集 / 综艺
        """
        normalized_values = [value.lower() for value in (meta_values or [])]
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
            if self._contains_any_keyword(normalized_values, keywords):
                return self.SERIES_CATEGORIES[category_key]
        # 默认兜底：大陆剧集
        return self.SERIES_CATEGORIES["mainland"]

    @staticmethod
    def _contains_any_keyword(values: List[str], keywords: List[str]) -> bool:
        for value in values:
            for keyword in keywords:
                if keyword.lower() in value:
                    return True
        return False

    @staticmethod
    def _extract_tvshow_meta(tvshow_nfo: Path) -> List[str]:
        """
        从 tvshow.nfo 提取 <country> / <genre> 内容
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
                        p.strip()
                        for p in text.replace("|", "/").replace(",", "/").replace("，", "/").split("/")
                        if p.strip()
                    ]
                    values.extend(parts)
        except Exception as e:
            logger.error(f"解析 NFO 失败：{tvshow_nfo}，错误：{str(e)}")
            return []
        return self._deduplicate(values)

    @staticmethod
    def _deduplicate(values: List[str]) -> List[str]:
        """
        列表去重并保持原顺序
        """
        uniq: List[str] = []
        seen = set()
        for value in values:
            if value not in seen:
                seen.add(value)
                uniq.append(value)
        return uniq

    def _safe_move_folder(self, src_dir: Path, dst_dir: Path) -> bool:
        """
        跨挂载点安全移动目录（必须使用 shutil.move）
        """
        if not src_dir.exists() or not src_dir.is_dir():
            logger.warning(f"{self.plugin_name} 源目录无效：{src_dir}")
            return False

        if dst_dir.exists():
            logger.warning(f"{self.plugin_name} 目标目录已存在，跳过：{dst_dir}")
            return False

        dst_dir.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.move(str(src_dir), str(dst_dir))
            logger.info(f"{self.plugin_name} 已移动：{src_dir} -> {dst_dir}")
            return True
        except Exception as e:
            logger.error(f"{self.plugin_name} 移动失败：{src_dir} -> {dst_dir}，错误：{str(e)}")
            return False
