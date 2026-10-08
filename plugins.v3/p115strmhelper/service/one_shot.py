from typing import Any, Callable, Dict, Optional

from app.sdk.logging import logger
from app.sdk.scheduler import add_plugin_once_job


def schedule_plugin_one_shot(
    service_id: str,
    name: str,
    func: Callable,
    func_kwargs: Optional[Dict[str, Any]] = None,
    delay_sec: int = 3,
    pid: str = "P115StrmHelper",
    provider_name: str = "115网盘STRM助手",
) -> bool:
    """通过 V3 SDK 注册插件一次性任务"""
    try:
        return add_plugin_once_job(
            plugin_id=pid,
            job_id=f"{pid}_{service_id}",
            func=func,
            name=name,
            delay_seconds=delay_sec,
            func_kwargs=func_kwargs or {},
        )
    except Exception as error:
        logger.error(f"【调度】注册一次性任务失败: {name}, {error}", exc_info=True)
        return False
