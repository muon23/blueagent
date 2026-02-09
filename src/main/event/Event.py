from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True, slots=True)
class Event:
    """
    Base event. Subclasses define payload fields.
    cursor: upstream resume token (e.g., Jetstream time_us or Firehose seq)
    """
    cursor: Optional[str] = None
