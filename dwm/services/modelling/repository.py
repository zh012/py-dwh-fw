from typing import Dict
from dwm.core import Repository, EventMixin, TabelDef
from dwm.core.orm import sql, select, Column, BigInteger, Integer, String, JSON, DateTime, Boolean, insert
from .datatype import ModelRepo, Model, MaterializationSpec


class ModelMixin:
    class Schema:
        model = TabelDef(
            Column("id", BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True),

            Column("name", String(255), unique=True),
            Column("category", String(255)),
            Column("description", String(128)),
            Column("slo", Integer)
            Column("sources", JSON, default=None)
            Column("mat_spec", JSON)

            Column("version", Integer, default=0)
            Column("created_at", DateTime, default=sql.func.now()),
            Column('updated_at', DateTime, default=sql.func.now(), onupdate=sql.func.now()),
            Column("build_no", String(255), default=None)
            Column("build_version", String(255), default=None)
            Column("build_time", DateTime, default=None)
            Column("ongoing_build_no", String(255), default=None)
        )

    def get_model(self, name: str) -> Model:
        t = self.T.model
        row = self.first(select([t]).where(t.name == name))
        return self._data_to_model(dict(row))

    def create_model(self, model: Model):
        data = self._model_to_data(model)
        self.q(self.T.model.insert().values(**data))

    def update_model(self, model: Model, on_version: int=None):
        MODEL = self.T.model
        data = self._model_to_data(model)
        query = MODEL.update().values(**data).where(MODEL.c.name == model.name)
        if on_version:
            query = query.where(MODEL.c.version == on_version)
        self.q(query)

    def _model_to_data(self, model: Model):
        data = model.dict_se(exclude={mat_spec}, exclude_unset=True)
        data['mat_spec'] = model.mat_spec.dict_se()
        data['mat_spec'][model.mat_spec.__concrete_type_key__] = mode.mat_spec.__class__.__name__
        return data

    def _data_to_model(self, data: Dict):
        d = data.copy()
        d['mat_spec'] = MaterializationSpec.factory(**d.pop('mat_spec'))
        return Model(**d)


class ModelRepoImp(ModelMixin, EventMixin, Repository, ModelRepo):
    pass
