from typing import Any

from app.sdk.logging import logger


class TransferChainPatcher:
    """保留调用入口，将 V3 整理执行交给宿主持久化队列"""

    @classmethod
    def enable(cls, task_manager: Any, handler: Any, storage_module: str) -> None:
        """记录宿主原生整理策略，不替换宿主方法"""
        logger.info("【整理接管】MoviePilot V3 使用原生持久化整理队列")

    @classmethod
    def disable(cls) -> None:
        """兼容服务停止时的清理调用"""
        return None
