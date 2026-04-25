import re
import shutil
import threading
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from apscheduler.triggers.cron import CronTrigger

from app.log import logger
from app.plugins import _PluginBase


class TMMMover(_PluginBase):
    """
    TMM 元数据智能转移助手
    """

    plugin_name = "TMM 元数据转移助手"
    plugin_desc = (
        "根据 TMM NFO 自动分拣迁移，并精准模拟 MoviePilot 原生图文入库通知"
    )
    plugin_version = "2.0.2"
    plugin_author = "QB"
    author_url = "https://github.com/TimeStandStill/MoviePilot-Plugins"
    plugin_icon = "sync.png"
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
        self._notify_enabled: bool = False  # 通知开关状态

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
        self._notify_enabled = config.get("notify_enabled", False)

        movie_ready = bool(self._source_movie_path and self._default_movie_path)
        series_ready = bool(self._source_series_path and self._default_series_path)
        self._enabled = bool(movie_ready or series_ready)

        logger.info(
            f"【TMM转移助手 2.0.3】配置已加载: 电影就绪={movie_ready}, 剧集就绪={series_ready}, 状态={'启用' if self._enabled else '未完全配置'}, 伪装通知={'开启' if self._notify_enabled else '关闭'}"
        )

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        在插件列表页的卡片上提供快捷运行按钮
        """
        return [
            {
                "cmd": "plugin/TMMMover/run",
                "method": "post",
                "text": "立即运行",
                "icon": "mdi-play",
                "color": "primary",
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        """
        提供外部与前端命令调用的 API 接口
        """
        return [
            {
                "path": "/run",
                "endpoint": self.api_run_once,
                "auth": "bear",  # 必须包含验证
                "methods": ["POST"],
                "summary": "手动触发 TMM 转移任务",
            }
        ]

    def get_form(self) -> Tuple[Optional[List[dict]], Dict[str, Any]]:
        """
        插件配置表单（Vuetify） - 100% 保持 1.1.12 原结构
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
                                            "hint": "包含电影文件的来源目录",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
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
                                            "hint": "包含剧集文件的来源目录",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
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
                                            "placeholder": "/media/movies",
                                        },
                                    }
                                ],
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
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
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
                                            "placeholder": "0 * * * *",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "notify_enabled",
                                            "label": "启用精美入库通知",
                                            "color": "primary",
                                            "hint": "开启后，每次物理转移成功会发送模拟 MP 原生的精美单条入库图文通知",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    }
                ],
            }
        ]

        model = {
            "source_movie_path": "",
            "source_series_path": "",
            "default_movie_path": "",
            "default_series_path": "",
            "cron": "0 * * * *",
            "notify_enabled": False,
        }
        return form, model

    def get_page(self) -> Optional[List[dict]]:
        """
        获取插件详情页面，展示手动运行按钮 - 100% 保持原结构
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
                                    "class": "mb-4",
                                },
                            },
                            {
                                "component": "div",
                                "props": {"class": "text-h6 mb-2"},
                                "text": "手动执行迁移任务",
                            },
                            {
                                "component": "div",
                                "props": {
                                    "class": "text-body-2 text-medium-emphasis mb-6 text-center"
                                },
                                "text": "插件已升级至 2.0 静默模式。未刮削及未规范重命名的目录将被安全跳过。只有发生实际转移的资源会触发单条入库通知，所有记录均在系统日志中保存。",
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
            logger.error(f"【TMM转移助手】cron 非法({self._cron})：{str(e)}")
            return []

        return [
            {
                "id": "scan_move_job",
                "name": "TMM 目录转移任务",
                "trigger": trigger,
                "func": self.run_once,
            }
        ]

    def stop_service(self):
        """
        退出插件时执行（MP 强制要求实现的抽象方法）
        """
        pass

    def api_run_once(self) -> Dict[str, Any]:
        """
        供前端页面调用的手动触发接口
        """
        logger.info("【TMM转移助手】收到手动运行指令，正在后台启动任务...")
        try:
            threading.Thread(target=self.run_once, daemon=True).start()
            return {
                "code": 0,
                "msg": "✅ 任务已在后台启动，详细进度请前往【系统日志】查看！",
            }
        except Exception as e:
            logger.error(f"【TMM转移助手】启动任务异常: {str(e)}", exc_info=True)
            return {"code": 1, "msg": f"启动失败: {str(e)}"}

    def run_once(self) -> str:
        """
        核心执行逻辑 (移除原有全局汇总通知推送，改为日志静默输出)
        """
        if not self.get_state():
            msg = "未完全配置，请至少在插件设置中填写【电影】或【剧集】的源目录和目标目录，并保存开启！"
            logger.warning(f"【TMM转移助手】{msg}")
            return msg

        logger.info(f"【TMM转移助手】=== 开始执行后台扫描任务 ===")
        
        movie_res = self._scan_source_dir(self._source_movie_path, "movie")
        series_res = self._scan_source_dir(self._source_series_path, "series")

        # 汇总名单
        final_res = {
            "moved": movie_res["moved"] + series_res["moved"],
            "skipped_invalid": movie_res["skipped_invalid"] + series_res["skipped_invalid"],
            "skipped_exists": movie_res["skipped_exists"] + series_res["skipped_exists"],
            "errors": movie_res["errors"] + series_res["errors"],
        }

        # 生成日志文本
        summary_text = self._build_notification_text(final_res)

        logger.info(f"【TMM转移助手】后台任务执行完成！成功转移: {len(final_res['moved'])} 个, 目标已存在: {len(final_res['skipped_exists'])} 个, 未规范: {len(final_res['skipped_invalid'])} 个, 失败: {len(final_res['errors'])} 个。")
        logger.info(f"【TMM转移助手】完整执行报告（静默模式）：\n{summary_text}")

        return summary_text.replace("\n", " ")
        
    def _build_notification_text(self, res: Dict[str, List[str]]) -> str:
        """
        原 1.1.12 版日志生成器，现仅用于生成后台 log 结构化文本
        """
        lines = ["后台扫描与转移任务已执行完成。\n"]

        lines.append(f"✅ 成功转移: {len(res['moved'])} 个")
        for item in res['moved']:
            lines.append(f"  - {item}")
        
        def _format_limited_list(item_list: List[str], max_show: int = 3, max_len: int = 20):
            res_lines = []
            for item in item_list[:max_show]:
                display_name = item[:max_len] + "..." if len(item) > max_len else item
                res_lines.append(f"  - {display_name}")
            return res_lines

        lines.append(f"\n⏭️ 跳过未规范/未刮削: {len(res['skipped_invalid'])} 个")
        lines.extend(_format_limited_list(res['skipped_invalid']))

        lines.append(f"\n⚠️ 目标已存在: {len(res['skipped_exists'])} 个")
        lines.extend(_format_limited_list(res['skipped_exists']))

        lines.append(f"\n❌ 转移失败: {len(res['errors'])} 个")
        lines.extend(_format_limited_list(res['errors']))

        return "\n".join(lines)

    def _scan_source_dir(self, source_path: str, mode: str) -> Dict[str, List[str]]:
        res = {
            "moved": [],
            "skipped_invalid": [],
            "skipped_exists": [],
            "errors": []
        }
        
        if not source_path:
            return res

        source_dir = Path(source_path)
        if not source_dir.exists() or not source_dir.is_dir():
            logger.warning(f"【TMM转移助手】来源目录无效或不存在，跳过: {source_dir}")
            return res

        logger.info(f"【TMM转移助手】正在扫描 ({mode} 模式): {source_dir}")

        for child in source_dir.iterdir():
            if not child.is_dir():
                continue

            # 拦截并删除 .deletedByTMM 目录
            if self._is_deleted_by_tmm_dir(child):
                try:
                    shutil.rmtree(child)
                    logger.info(f"【TMM转移助手】已自动清理 TMM 删除标记目录: {child.name}")
                except Exception as e:
                    logger.error(f"【TMM转移助手】清理 TMM 删除标记目录失败 [{child.name}]: {str(e)}")
                continue

            try:
                status = self._process_one_folder(child, mode)
                if status == "MOVED":
                    res["moved"].append(child.name)
                elif status == "SKIPPED_INVALID":
                    res["skipped_invalid"].append(child.name)
                elif status == "SKIPPED_EXISTS":
                    res["skipped_exists"].append(child.name)
                elif status == "ERROR":
                    res["errors"].append(child.name)
            except Exception as e:
                res["errors"].append(child.name)
                logger.error(f"【TMM转移助手】处理子目录发生未捕获异常 [{child.name}]: {str(e)}")

        return res

    def _is_deleted_by_tmm_dir(self, folder: Path) -> bool:
        name = folder.name.strip()
        return name == ".deletedByTMM" or name.endswith(".deletedByTMM")

    def _has_year_in_name(self, folder_name: str) -> bool:
        return bool(re.search(r"[\(（]\d{4}[\)）]", folder_name))

    def _process_one_folder(self, folder: Path, mode: str) -> str:
        """
        处理单个目录（包含 2.0 版核心触发逻辑）
        """
        # 1. 重命名校验
        if not self._has_year_in_name(folder.name):
            logger.info(f"【TMM转移助手】未重命名规范 (未包含年份括号)，已安全跳过: {folder.name}")
            return "SKIPPED_INVALID"

        # 2. 刮削完成校验
        nfo_files = list(folder.glob("*.nfo"))
        if not nfo_files:
            logger.info(f"【TMM转移助手】未刮削完成 (无 NFO 文件)，已安全跳过: {folder.name}")
            return "SKIPPED_INVALID"

        if mode == "movie":
            target_root = Path(self._default_movie_path)
            category_name = ""
        else:
            tvshow_nfo = folder / "tvshow.nfo"
            if not tvshow_nfo.exists():
                for f in nfo_files:
                    if f.name.lower() == "tvshow.nfo":
                        tvshow_nfo = f
                        break

            if not tvshow_nfo.exists():
                logger.info(f"【TMM转移助手】剧集未刮削完成 (缺少主干 tvshow.nfo)，已跳过: {folder.name}")
                return "SKIPPED_INVALID"

            target_root = self._resolve_series_target_root(tvshow_nfo)
            category_name = target_root.name

        target_dir = target_root / folder.name
        
        # 依赖原汁原味的 _safe_move_folder 进行物理迁移
        status = self._safe_move_folder(folder, target_dir)

        # 2.0 新增挂载点：若真实移动成功，则触发单条精美通知
        if status == "MOVED" and self._notify_enabled:
            self._send_item_notification(target_dir, mode, category_name)

        return status

    def _send_item_notification(self, target_dir: Path, mode: str, category: str):
        """
        2.0.x 新增方法：解析 NFO 并发送伪装入库通知
        """
        try:
            nfo_file = None
            if mode == "series":
                nfo_file = target_dir / "tvshow.nfo"
            else:
                nfo_files = list(target_dir.glob("*.nfo"))
                if nfo_files:
                    for f in nfo_files:
                        if f.name.lower() != "tvshow.nfo":
                            nfo_file = f; break
                    if not nfo_file: nfo_file = nfo_files[0]
            if not nfo_file: return

            tree = ET.parse(nfo_file)
            root = tree.getroot()

            title = root.findtext("title") or target_dir.name
            year = root.findtext("year") or ""
            plot = root.findtext("plot") or "暂无简介"
            if len(plot) > 150: plot = plot[:150] + "..."

            rating = "0.0"
            rating_node = root.find(".//rating")
            if rating_node is not None:
                rating = rating_node.text or rating_node.findtext("value") or "0.0"
            try: rating = f"{float(rating):.1f}"
            except: rating = "0.0"

            if mode == "movie" and not category:
                category = root.findtext("genre") or ""

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

            # 头图精准匹配 (fanart)
            image_path = ""
            if mode == "movie":
                fanart_name = f"{target_dir.name}-fanart"
                for ext in [".jpg", ".png", ".jpeg"]:
                    test_path = target_dir / (fanart_name + ext)
                    if test_path.exists():
                        image_path = str(test_path); break
                if not image_path:
                    for name in ["fanart.jpg", "fanart.png", "poster.jpg"]:
                        if (target_dir / name).exists():
                            image_path = str(target_dir / name); break
            else:
                for name in ["fanart.jpg", "fanart.png", "poster.jpg"]:
                    if (target_dir / name).exists():
                        image_path = str(target_dir / name); break

            res_term = ""
            if "4k" in target_dir.name.lower() or "2160p" in target_dir.name.lower(): res_term = "4K"
            elif "1080p" in target_dir.name.lower(): res_term = "1080p"

            msg_title = f"《{title} ({year})》 已入库 ✅"
            msg_text = (
                f"⭐️评分：{rating} ｜ 🎬类型：{'电影' if mode=='movie' else '剧集'}"
                f"{' ｜ 📁类别：'+category if category else ''}"
                f"{' ｜ 📦质量：'+res_term if res_term else ''}\n\n"
                f"📝简介：{plot}\n\n"
                f"📄共 {file_count} 个文件 ｜ 💾大小：{total_size}"
            )

            self.post_message(title=msg_title, text=msg_text, image=image_path)

        except Exception as e:
            logger.error(f"【TMM转移助手】发送独立通知失败 [{target_dir.name}]: {str(e)}")

    def _resolve_series_target_root(self, tvshow_nfo: Path) -> Path:
        """
        1.1.12 原逻辑完整保留
        """
        meta_values = self._extract_tvshow_meta(tvshow_nfo)
        category_name = self._resolve_series_category_name(meta_values)
        return Path(self._default_series_path) / category_name

    def _resolve_series_category_name(self, meta_values: List[str]) -> str:
        """
        1.1.12 原逻辑完整保留
        """
        normalized_values = [value.lower() for value in meta_values]

        category_rules = [
            ("anime", ["动漫", "动画", "anime", "animation", "卡通", "cartoon"]),
            ("shortdrama", ["短剧", "微短剧", "短片"]),
            ("documentary", ["纪录片", "纪录", "documentary"]),
            ("variety", ["综艺", "真人秀", "脱口秀", "variety", "reality"]),
            ("hktw", ["香港", "台湾", "港台", "港剧", "台剧"]),
            ("jpkr", ["日本", "韩国", "日韩", "日剧", "韩剧"]),
            ("mainland", ["中国大陆", "中国", "大陆", "内地", "国产", "华语"]),
            ("western", ["美国", "英国", "欧美", "欧洲", "加拿大", "澳大利亚", "法国", "德国", "意大利", "西班牙", "俄罗斯", "以色列", "北欧", "中东"]),
        ]

        for category_key, keywords in category_rules:
            if any(
                keyword.lower() in val
                for val in normalized_values
                for keyword in keywords
            ):
                return self.SERIES_CATEGORIES[category_key]

        return self.SERIES_CATEGORIES["western"]

    def _extract_tvshow_meta(self, tvshow_nfo: Path) -> List[str]:
        """
        1.1.12 原逻辑完整保留
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
                        for p in text.replace("|", "/")
                        .replace(",", "/")
                        .replace("，", "/")
                        .split("/")
                        if p.strip()
                    ]
                    values.extend(parts)
        except Exception as e:
            logger.error(f"【TMM转移助手】解析 NFO 失败 {tvshow_nfo}: {str(e)}")
            return []

        return self._deduplicate(values)

    def _deduplicate(self, values: List[str]) -> List[str]:
        """
        1.1.12 原逻辑完整保留
        """
        uniq = []
        seen = set()
        for value in values:
            if value not in seen:
                seen.add(value)
                uniq.append(value)
        return uniq

    def _safe_move_folder(self, src_dir: Path, dst_dir: Path) -> str:
        """
        1.1.12 原逻辑完整保留 (返回最原始的 MOVED / SKIPPED_EXISTS / ERROR 状态字)
        """
        if dst_dir.exists():
            logger.info(f"【TMM转移助手】目标目录已存在，跳过覆盖: {dst_dir.name}")
            return "SKIPPED_EXISTS"

        dst_dir.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.move(str(src_dir), str(dst_dir))
            logger.info(f"【TMM转移助手】✔ 成功移动: [{src_dir.name}] -> [{dst_dir.parent.name}]")
            return "MOVED"
        except Exception as e:
            logger.error(f"【TMM转移助手】❌ 移动失败 [{src_dir.name}]: {str(e)}")
            if dst_dir.exists() and src_dir.exists():
                logger.error(f"【TMM转移助手】⚠️ 发生不完整迁移，请手动检查: {dst_dir}")
            return "ERROR"
