from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, List


@dataclass(frozen=True, slots=True)
class Event:
    """
    Base event model for all pipeline events.

    Attributes:
        cursor: Upstream resume token (for example, Jetstream `time_us`).
    """
    cursor: Optional[str] = None


class EventFilter(ABC):
    """Base interface for event filters used by ingestors."""

    @abstractmethod
    def apply(self, events: List[Event]) -> List[Event]:
        """
        Filter a batch of events.

        Args:
            events: Input events to filter.

        Returns:
            Filtered events that should continue downstream.

        Raises:
            Exception: Implementations may raise when filter evaluation fails.
        """
        ...
