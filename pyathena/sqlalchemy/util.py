"""Utility classes for PyAthena SQLAlchemy dialect."""


class _HashableDict(dict):  # type: ignore[type-arg]
    """A dictionary subclass that can be used as a dictionary key.

    SQLAlchemy's reflection caching requires hashable objects. This class
    enables dictionary values (like table properties) to be cached by
    making them hashable through tuple conversion.
    """

    def __hash__(self):  # type: ignore[override]
        return hash(tuple(sorted(self.items())))
