"""
models.py — Pydantic data models for the monitoring pipeline.
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class MessageType(str, Enum):
    """Classification of a parsed message."""
    OPEN = "OPEN"
    CLOSE = "CLOSE"
    RWT = "RWT"        # Type-B / MENSAJE that isn't open/close
    SINGLE = "SINGLE"  # Unmatched content


class RawMessage(BaseModel):
    """Ingress-level representation straight from Telegram."""
    telegram_id: int
    raw_text: str
    receive_timestamp: datetime


class ParsedMessage(BaseModel):
    """Fully processed message ready for persistence."""
    telegram_id: int
    telefono: str
    estacion: str
    red: Optional[str] = None
    tipo_mensaje: MessageType
    canal: Optional[str] = None
    texto: str
    timestamp: datetime  # Internal msg timestamp (date + time from content)
    tono: bool = False
