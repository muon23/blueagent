import sys
from typing import Sequence, Iterable

from event.Event import Event
from sinks.Sink import Sink


class MultiSink(Sink):
    """
    Fan-out to multiple sinks.
    - If a critical sink fails, MultiSink fails (and you should NOT advance cursor).
    - Optional sinks can be marked best-effort.

    This class does not register handlers; it simply forwards write/flush/close.
    """
    def __init__(self, critical: Sequence[Sink], optional: Sequence[Sink] = ()) -> None:
        super().__init__()
        self.critical = list(critical)
        self.optional = list(optional)

    async def write(self, events: Iterable[Event]) -> None:
        batch = list(events)  # materialize once
        # critical sinks must succeed
        for s in self.critical:
            await s.write(batch)
        # optional sinks are best-effort
        for s in self.optional:
            try:
                await s.write(batch)
            except Exception as e:
                print(f"[MultiSink] optional sink {s.__class__.__name__} failed: {e}", file=sys.stderr)

    async def flush(self) -> None:
        for s in self.critical:
            await s.flush()
        for s in self.optional:
            try:
                await s.flush()
            except Exception as e:
                print(f"[MultiSink] optional sink {s.__class__.__name__} flush failed: {e}", file=sys.stderr)

    async def close(self) -> None:
        for s in self.critical:
            await s.close()
        for s in self.optional:
            try:
                await s.close()
            except Exception as e:
                print(f"[MultiSink] optional sink {s.__class__.__name__} close failed: {e}", file=sys.stderr)
