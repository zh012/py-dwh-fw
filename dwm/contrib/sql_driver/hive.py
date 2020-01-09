import time
from pyhive import hive
from TCLIService.ttypes import TOperationState as ThriftState

from .base import QueryStatus, QueryResult, BaseAsyncQueryResult, BaseDriver


class HiveAsyncQueryResult(BaseAsyncQueryResult):
    def __init__(self, cursor):
        self._cursor = cursor
        self._last_status = None
        self._result = None

    def poll(self):
        self._last_status = status = self._cursor.poll()
        if status.operationState in [
            ThriftState.INITIALIZED_STATE,
            ThriftState.RUNNING_STATE,
            ThriftState.PENDING_STATE,
        ]:
            return QueryStatus.PROCESSING
        if status.errorMessage or status.operationState not in [
            ThriftState.FINISHED_STATE
        ]:
            return QueryStatus.FAILED
        return QueryStatus.SUCCEEDED

    def get(self, poll_interval: int = 1, max_polls: int = None) -> QueryResult:
        if self._result:
            return self._result

        while max_polls is None or max_polls > 0:
            if max_polls is not None:
                max_polls -= 1
            status = self.poll()
            if status == QueryStatus.PROCESSING:
                time.sleep(poll_interval)
                continue

            if status == QueryStatus.SUCCEEDED:
                self._result = QueryResult(
                    rows=self._cursor.fetchall(),
                    description=self._cursor.description,
                    status=status,
                    message=None,
                )
            elif status == QueryStatus.FAILED:
                self._result = QueryResult(
                    rows=[],
                    status=status,
                    description=[],
                    message=self._last_status.errorMessage,
                )
            self._cursor.close()
            return self._result

        raise TimeoutError("The query did not finish in time")


class HiveDriver(BaseDriver):
    def __init__(self, host: str, port: int, username: str, **config):
        self._host = host
        self._port = port
        self._username = username
        self._config = config
        self._connection = None
        self._cursor = None

    @property
    def connection(self):
        if not self._connection:
            self._connection = hive.connect(
                host=self._host,
                port=self._port,
                username=self._username,
                configuration=self._config,
            )
        return self._connection

    def reset_connection(self):
        if self._connection:
            self._connection.close()
            self._connection = None

    def cursor(self):
        return self.connection.cursor()

    def query(self, sql: str) -> QueryResult:
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql)
            return QueryResult(
                rows=cursor.fetchall(),
                description=cursor.description,
                status=QueryStatus.SUCCEEDED,
                message=None,
            )
        except Exception as e:
            return QueryResult(
                rows=[], description=[], status=QueryStatus.FAILED, message=str(e)
            )
        finally:
            cursor.close()

    def async_query(self, sql: str) -> BaseAsyncQueryResult:
        cursor = self.connection.cursor()
        cursor.execute(sql, async_=True)
        return HiveAsyncQueryResult(cursor)
