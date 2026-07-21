class EntryWindow:
    """Represents whether an opportunity is early, valid, or too late."""

    def __init__(self, open_time=None, close_time=None, active=False):
        self.open_time = open_time
        self.close_time = close_time
        self.active = active

    def is_open(self) -> bool:
        return self.active
