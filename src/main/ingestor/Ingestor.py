from abc import ABC, abstractmethod
from typing import List, Dict, Any


class Ingestor(ABC):
    @abstractmethod
    def wanted_collections(self) -> List[str]:
        pass

    @abstractmethod
    async def handle_event(self, evt: Dict[str, Any]) -> None:
        """
        evt is a Jetstream event dict (already JSON-decoded).
        """
        pass

    async def flush(self) -> None:
        """Optional: periodic batch flush to DB / queue."""
        return
