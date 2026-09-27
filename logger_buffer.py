"""
In-Memory Ring Buffer Logging Handler.
Captures recent log messages and errors for inspection via REST API and Web Dashboard on Render.
"""

from collections import deque
from datetime import datetime, timezone
import logging
from typing import Any, Dict, List, Optional


class InMemoryLogHandler(logging.Handler):
    """
    Thread-safe in-memory ring buffer for the last N log entries.
    Allows filtering by level (e.g. WARNING, ERROR) and logger name.
    """

    def __init__(self, capacity: int = 500):
        super().__init__()
        self.capacity = capacity
        self.buffer: deque = deque(maxlen=capacity)

    def emit(self, record: logging.LogRecord):
        try:
            entry = {
                "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
                "created": record.created,
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            }
            if record.exc_info and record.exc_text:
                entry["exception"] = record.exc_text
            self.buffer.append(entry)
        except Exception:
            self.handleError(record)

    def get_logs(
        self,
        level: Optional[str] = None,
        logger_name: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Returns filtered log records in reverse chronological order (newest first)."""
        res = []
        target_level = level.upper() if level else None
        target_logger = logger_name.lower() if logger_name else None

        # Level hierarchy for filtering (e.g., if level=WARNING, return WARNING, ERROR, CRITICAL)
        level_values = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL,
        }
        min_level_no = level_values.get(target_level, 0) if target_level else 0

        # Snapshot of buffer
        items = list(self.buffer)
        for item in reversed(items):
            item_level_no = level_values.get(item["level"], 0)
            if min_level_no and item_level_no < min_level_no:
                continue
            if target_logger and target_logger not in item["logger"].lower():
                continue
            res.append(item)
            if len(res) >= limit:
                break
        return res

    def get_errors(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Returns only WARNING, ERROR, and CRITICAL logs."""
        return self.get_logs(level="WARNING", limit=limit)

    def clear(self):
        self.buffer.clear()


# Global singleton instance
log_handler = InMemoryLogHandler(capacity=600)
