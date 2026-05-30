"""Object-oriented wrapper around swsscommon.Select.

Provides a simple event loop that dispatches selectable events to registered
handler callbacks, matching the pattern used by SONiC manager daemons.
"""

from __future__ import annotations

from typing import Any, Callable

from common.swss import swsscommon

# Handler signature: receives the selectable, returns STOP sentinel to exit or None.
Handler = Callable[[Any], "object | None"]


class SelectLoop:
    """Event loop dispatching swsscommon.Select events to registered handlers.

    Usage::

        loop = SelectLoop()
        loop.add(consumer, handle_update)
        loop.run()
    """

    STOP: object = object()
    """Return this from a handler to terminate the event loop."""

    def __init__(self) -> None:
        self._selector = swsscommon.Select()
        self._handlers: dict[int, Handler] = {}

    def add(self, selectable: Any, handler: Handler) -> Any:
        """Register a selectable with its event handler.

        Args:
            selectable: A swsscommon selectable (ConsumerTable, etc.).
            handler: Callback invoked when the selectable is ready.

        Returns:
            The selectable (for chaining convenience).
        """
        fd: int = selectable.getFd()
        self._selector.addSelectable(selectable)
        self._handlers[fd] = handler
        return selectable

    def dispatch_once(self) -> object | None:
        """Block until one event fires, then dispatch it.

        Returns:
            The handler's return value, or None if no handler matched.
        """
        state, selectable = self._selector.select()
        if state != swsscommon.Select.OBJECT:
            return None

        handler = self._handlers.get(selectable.getFd())
        if handler is None:
            raise RuntimeError(
                "no handler registered for selectable fd %s" % selectable.getFd()
            )
        return handler(selectable)

    def run(self) -> None:
        """Run the event loop until a handler returns STOP."""
        while True:
            result = self.dispatch_once()
            if result is self.STOP:
                return
