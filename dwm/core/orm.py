from abc import ABCMeta, abstractmethod
from typing import Union, no_type_check, ContextManager
from itertools import chain

try:
    from functools import cached_property
except:
    from .future import cached_property
from types import SimpleNamespace
from contextlib import contextmanager
from datetime import datetime
from sqlalchemy import *
from sqlalchemy import engine, sql
from .event import Event


class TableDef(tuple):
    def __new__(cls, *args):
        return tuple.__new__(TableDef, args)


class RepositoryMetaClass(ABCMeta):
    @no_type_check  # noqa C901
    def __new__(mcs, name, bases, namespace, **kwargs):  # noqa C901
        table_defs = {}

        for schema in filter(
            None,
            chain(
                (
                    getattr(base, "Schema", None)
                    for base in reversed(bases)
                    if issubclass(base, BaseRepository)
                ),
                (namespace.get("Schema"),),
            ),
        ):
            table_defs.update(
                {k: v for k, v in schema.__dict__.items() if isinstance(v, TableDef)}
            )

        metadata = MetaData()
        for name, table_def in table_defs.items():
            Table(name, metadata, *table_def)

        new_namespace = namespace.copy()
        new_namespace.update(M=metadata)
        return super().__new__(mcs, name, bases, new_namespace, **kwargs)


class Repository(metaclass=RepositoryMetaClass):
    def __init__(self, bind: Union[str, engine.Engine, engine.Connection]):
        if isinstance(bind, str):
            self.bind = create_engine(bind)
        else:
            self.bind = bind

    @cached_property
    def T(self):
        return SimpleNamespace(**self.M.tables)

    def q(self, query: sql.base.Executable):
        return self.bind.execute(query)

    def first(self, query: sql.base.Executable):
        return next(self.q(query), None)

    def create_all_tables(self):
        self.M.create_all(bind=self.bind)

    def drop_all_tables(self):
        self.M.drop_all(bind=self.bind)

    @contextmanager
    def transaction(self):
        connection = self.bind.connect()
        txn = self.__class__(connection)
        transaction = connection.begin()
        try:
            yield txn
            transaction.commit()
        except:
            transaction.rollback()
            raise
        finally:
            connection.close()


class EventMixin:
    class Schema:
        event = TableDef(
            Column("id", BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True),
            Column("event_name", String(255)),
            Column("domain", String(255)),
            Column("domain_id", String(128)),
            Column("data", JSON),
            Column("created_at", DateTime, default=sql.func.now()),
        )

    def append_event(self, event: Event):
        data = event.dict(include={"domain", "domain_id"})
        data["event_name"] = event.__class__.__name__
        data["data"] = event.dict_se(exclude={"domain", "domain_id", "event_name"})
        data["created_at"] = datetime.utcnow()
        self.q(self.T.event.insert().values(**data))
