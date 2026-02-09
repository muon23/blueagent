from typing import Callable, Awaitable, Dict, Type, Optional, cast, Iterable, TypeVar

from event.Event import Event


class Sink:
    """
    A Sink dispatches events to registered handlers keyed by event class.

    Notes:
    - Dispatch is by exact type with fallback to base-class handlers (MRO scan).
    - Unknown events raise by default (fail-fast). Override for best-effort sinks.
    """
    Handler = Callable[[Event], Awaitable[None]]
    E = TypeVar("E", bound="Event")

    def __init__(self) -> None:
        self._handlers: Dict[Type[Event], Sink.Handler] = {}

    def on(self, event_type: Type[E], handler: Callable[[E], Awaitable[None]]) -> None:
        # store erased handler
        self._handlers[event_type] = handler  # type: ignore[assignment]

    def _resolve_handler(self, ev: Event) -> Optional[Handler]:
        t: Type[Event] = type(ev)

        h = self._handlers.get(t)
        if h is not None:
            return h

        for base in t.__mro__[1:]:
            if issubclass(base, Event):
                h = self._handlers.get(cast(Type[Event], base))
                if h is not None:
                    return h

        return None

    async def write(self, events: Iterable[Event]) -> None:
        for ev in events:
            h = self._resolve_handler(ev)
            if h is None:
                raise NotImplementedError(f"{self.__class__.__name__} has no handler for {type(ev).__name__}")
            await h(ev)

    async def flush(self) -> None:
        return

    async def close(self) -> None:
        return
