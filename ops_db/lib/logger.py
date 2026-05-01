"""ops_db 日志模块。"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

LOG_DIR = Path.home() / ".ops_db" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# 敏感词（用于日志脱敏）
SENSITIVE_KEYWORDS = ["password", "passwd", "secret", "credential", "token"]


def _mask_sensitive(text: str) -> str:
    """日志脱敏。"""
    result = text
    for kw in SENSITIVE_KEYWORDS:
        import re
        result = re.sub(
            rf'({kw}["\s:=]+)([^\s",}}]+)',
            r"\1***",
            result,
            flags=re.IGNORECASE,
        )
    return result


class SensitiveFilter(logging.Filter):
    """过滤敏感信息。"""

    def filter(self, record: logging.LogRecord) -> bool:
        if record.msg and isinstance(record.msg, str):
            record.msg = _mask_sensitive(record.msg)
        if record.args:
            record.args = tuple(
                _mask_sensitive(str(a)) if isinstance(a, str) else a
                for a in record.args
            )
        return True


def get_logger(name: str) -> logging.Logger:
    """返回带文件输出的 logger。"""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s [%(name)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # File handler
    today = datetime.now().strftime("%Y%m%d")
    fh = logging.FileHandler(LOG_DIR / f"ops_db_{today}.log", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    fh.addFilter(SensitiveFilter())
    logger.addHandler(fh)

    return logger
