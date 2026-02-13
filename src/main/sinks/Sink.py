from typing import Callable, Awaitable, Dict, Type, Optional, cast, Iterable, TypeVar

from events.Event import Event


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
        """
        Initialize an empty event handler registry.

        Args:
            None.

        Returns:
            None.

        Raises:
            None.
        """
        self._handlers: Dict[Type[Event], Sink.Handler] = {}

    def on(self, event_type: Type[E], handler: Callable[[E], Awaitable[None]]) -> None:
        """
        Register an async handler for an event type.

        Args:
            event_type: Event class handled by `handler`.
            handler: Async handler callback for the event type.

        Returns:
            None.

        Raises:
            None.
        """
        # store erased handler
        self._handlers[event_type] = handler  # type: ignore[assignment]

    def _resolve_handler(self, ev: Event) -> Optional[Handler]:
        """
        Resolve best matching handler using exact type then MRO fallback.

        Args:
            ev: Event instance to resolve handler for.

        Returns:
            Matched handler or `None` when no handler is registered.

        Raises:
            None.
        """
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
        """
        Dispatch events to registered handlers.

        Args:
            events: Event iterable to write.

        Returns:
            None.

        Raises:
            NotImplementedError: When an event has no matching handler.
            Exception: Propagates handler execution errors.
        """
        for ev in events:
            h = self._resolve_handler(ev)
            if h is None:
                raise NotImplementedError(f"{self.__class__.__name__} has no handler for {type(ev).__name__}")
            await h(ev)

    async def flush(self) -> None:
        """
        Flush buffered writes for sink implementations that batch.

        Args:
            None.

        Returns:
            None.

        Raises:
            Exception: Implementations may raise flush errors.
        """
        return

    async def close(self) -> None:
        """
        Close sink resources.

        Args:
            None.

        Returns:
            None.

        Raises:
            Exception: Implementations may raise close errors.
        """
        return
