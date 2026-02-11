from abc import ABC, abstractmethod
from dataclasses import dataclass

from typing import List, Dict, Any, Optional
from events.Event import EventFilter

from events.Event import Event


@dataclass
class Ingestor(ABC):
    filters: Optional[List[EventFilter]] = None

    @abstractmethod
    def wanted_collections(self) -> List[str]:
        pass

    @abstractmethod
    async def handle_event(self, evt: Dict[str, Any]) -> List[Event]:
        """
        evt is a Jetstream event dict (already JSON-decoded).
        """
        pass

    async def flush(self) -> None:
        """Optional: periodic batch flush to DB / queue."""
        return

    def _apply_filters(self, events: List[Any]) -> List[Any]:
        if not self.filters:
            return events
        filtered = events
        for f in self.filters:
            filtered = f.apply(filtered)
        return filtered
