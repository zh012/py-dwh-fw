from typing import Set
from hashlib import sha1
from enum import Enum
from abc import ABC, abstractmethod
from datetime import datetime
from dwm.core import DataClass, Repository, Event


class MaterializationSpec(DataClass, polymorph=True):
    pass


class DatalakeSourceMatSpec(MaterializationSpec):
    target_name: str
    is_external_table: bool = True
    datalake_dataset_id: str


class CtasMatSpec(MaterializationSpec):
    target_name: str
    select: str


class ModelDef(DataClass):
    name: str
    category: str  # sementic category names: source, base, dimension, fact, analytics or report
    description: str = None
    slo: int = 0
    sources: Set[str] = None
    mat_spec: MaterializationSpec

    def is_source(self):
        return not self.dependencies


class ModelInfo(DataClass):
    version: int = None
    created_at: datetime = None
    updated_at: datetime = None
    build_no: str = None
    build_version: int = None
    build_time: datetime = None
    ongoing_build_no: str = None


class Model(ModelInfo, ModelDef):
    pass


class ModelRepo(ABC):
    @abstractmethod
    def transaction(self) -> ContextManager["ModelRepo"]:
        pass

    @abstractmethod
    def append_event(self, event: Event):
        pass

    @abstractmethod
    def get_model(self, name: str) -> Model:
        pass

    @abstractmethod
    def create_model(self, model: Model):
        pass

    @abstractmethod
    def update_model(self, model: Model, on_version: int = None):
        pass
