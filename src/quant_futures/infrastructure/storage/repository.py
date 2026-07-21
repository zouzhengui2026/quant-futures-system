"""Repository abstractions used by domain services."""

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

T = TypeVar("T")


class Repository(ABC, Generic[T]):
    """Base repository contract."""

    @abstractmethod
    def save(self, item: T) -> None:
        raise NotImplementedError
