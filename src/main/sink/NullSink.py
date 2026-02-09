from event.Event import Event
from sink.Sink import Sink


class NullSink(Sink):
    """No-op sink for tests / dry runs."""

    def __init__(self) -> None:
        super().__init__()

        # Register a default handler for all Event types via base class
        async def _noop(_: Event) -> None:
            return

        self.on(Event, _noop)
