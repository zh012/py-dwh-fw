from functools import wraps
import logging
from datetime import datetime
from typing import Dict, Any, Union

from pydantic import *

logger = logging.getLogger("core.data")


class DataClassUtil:
    def dict_se(self, *args, **kwargs):
        def process(data):
            for k, v in data.items():
                if isinstance(v, datetime):
                    data[k] = v.isoformat()
                elif isinstance(v, Dict):
                    process(v)

        return process(self.dict(*args, **kwargs))


class AnyData(DataClassUtil, BaseModel):
    class Config:
        extra = Extra.allow


class DataClass(DataClassUtil, BaseModel):
    def __init_subclass__(
        cls,
        polymorph: bool = False,
        concrete_type_key: str = "__concrete_type__",
        **kwargs,
    ):
        super().__init_subclass__(**kwargs)
        cls.register(AnyData) # TODO this does not work properly. leaf subclasses are not registered. dont know why

        if polymorph:
            cls.__concrete_types__ = {}
            cls.__concrete_type_key__ = concrete_type_key

            def __init_subclass__(concrete_type, **kwargs):
                super().__init_subclass__(**kwargs)
                name = concrete_type.__name__
                if name in cls.__concrete_types__:
                    logger.warning(
                        f"concrete type {name} of {cls.__name__} was defined multiple times"
                    )
                cls.__concrete_types__[concrete_type.__name__] = concrete_type

            cls.__init_subclass__ = classmethod(__init_subclass__)

            def factory(self, **data: Any) -> DataClass:
                if concrete_type_key not in data:
                    raise TypeError(
                        f"ploymorph factory missing 1 required argument: '{concrete_type_key}'"
                    )
                concrete_type = cls.__concrete_types__.get(
                    data[concrete_type_key], AnyData
                )
                return concrete_type(**data)

            cls.factory = classmethod(factory)

DataClass.register(AnyData)
