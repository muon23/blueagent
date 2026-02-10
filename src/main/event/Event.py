from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, List


@dataclass(frozen=True, slots=True)
class Event:
    """
    Base event. Subclasses define payload fields.
    cursor: upstream resume token (e.g., Jetstream time_us or Firehose seq)
    """
    cursor: Optional[str] = None


class EventFilter(ABC):
    @abstractmethod
    def apply(self, events: List[Event]) -> List[Event]:
        pass
