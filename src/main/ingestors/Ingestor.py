from abc import ABC, abstractmethod
from dataclasses import dataclass

from typing import List, Dict, Any, Optional
from events.Event import EventFilter

from events.Event import Event


@dataclass
class Ingestor(ABC):
    """
    Base class for transforming stream payloads into domain events.

    Implementations declare supported collections and parse each raw stream
    event into zero or more typed domain events.
    """

    filters: Optional[List[EventFilter]] = None

    @abstractmethod
    def wanted_collections(self) -> List[str]:
        """
        Return collection NSIDs this ingestor can process.

        Args:
            None.

        Returns:
            List of collection identifiers for stream subscription routing.

        Raises:
            None.
        """
        ...

    @abstractmethod
    async def handle_event(self, evt: Dict[str, Any]) -> List[Event]:
        """
        Transform one raw stream event into domain events.

        Args:
            evt: Jetstream event dictionary (already JSON-decoded).

        Returns:
            List of emitted domain events (possibly empty).

        Raises:
            Exception: Implementations may raise when parsing fails.
        """
        ...

    async def flush(self) -> None:
        """
        Flush ingestor-internal buffers if any.

        Args:
            None.

        Returns:
            None.

        Raises:
            Exception: Implementations may raise when flush fails.
        """
        return

    def _apply_filters(self, events: List[Any]) -> List[Any]:
        """
        Apply configured filters to a batch of events.

        Args:
            events: Events to filter.

        Returns:
            Filtered events.

        Raises:
            Exception: Propagates any filter errors.
        """
        if not self.filters:
            return events
        filtered = events
        for f in self.filters:
            filtered = f.apply(filtered)
        return filtered
