"""Small object-oriented wrapper around swsscommon.Select."""


class SelectLoop:
    STOP = object()

    def __init__(self, swsscommon):
        self._swsscommon = swsscommon
        self._selector = swsscommon.Select()
        self._handlers = {}

    def add(self, selectable, handler):
        fd = selectable.getFd()
        self._selector.addSelectable(selectable)
        self._handlers[fd] = handler
        return selectable

    def dispatch_once(self):
        state, selectable = self._selector.select()
        if state != self._swsscommon.Select.OBJECT:
            return None

        handler = self._handlers.get(selectable.getFd())
        if handler is None:
            raise RuntimeError("no handler registered for selectable fd %s" % selectable.getFd())
        return handler(selectable)

    def run(self):
        while True:
            result = self.dispatch_once()
            if result is self.STOP:
                return
