from abc import ABC, abstractmethod
from typing import Any, List, Tuple
from dataclasses import dataclass
from enum import Enum

from pyhive import hive
from TCLIService.ttypes import TOperationState as ThriftState


class QueryFailed(Exception):
    pass


class QueryStatus(Enum):
    PROCESSING = 100
    # all the succeeded code should be greater or equal to 300
    SUCCEEDED = 200
    # all the error code should be greater or equal to 300
    FAILED = 300

    def is_finished(self):
        return self.value >= 200

    def is_failed(self):
        return self.value >= 300

    def is_succeeded(self):
        return self.value >= 200 and self.value < 300


@dataclass
class QueryResult:
    description: List[Tuple[Any]]
    rows: List[Tuple[Any]]
    status: QueryStatus
    message: str

    def raise_on_failure(self):
        if self.status.is_failed():
            raise QueryFailed(self.status.value, self.message)


class BaseAsyncQueryResult(ABC):
    @abstractmethod
    def poll(self) -> QueryStatus:
        pass

    @abstractmethod
    def get(self, poll_interval: int = 1, max_polls: int = None) -> QueryResult:
        pass


class BaseDriver(ABC):
    @abstractmethod
    def query(self, sql: str) -> QueryResult:
        pass

    @abstractmethod
    def async_query(self, sql: str) -> BaseAsyncQueryResult:
        pass
