import time
from dwm.core import Command, CommandHandler

from .datatype import Model, ModelDef, ModelInfo, Event

command_handler = CommandHandler()


class ApplyModelCommand(ModelDef, Command):
    pass


class ModelCreated(Model, Event):
    pass


class ModelUpdated(Model, Event):
    pass


@command_handler(ApplyModelCommand)
def apply_model_command_handler(command: ApplyModelCommand, *, repo: ModelRepo):
    old_model = repo.get_model(command.name)

    if not old_model:
        model = Model(
            version = 1,
            created_at = time.time(),
            **command.dict()
        )
        event = ModelCreated(
            domain = 'Modle',
            domain_id = model.name,
            **model.dict()
        )
        with repo.transaction() as txn:
            txn.create_model(model)
            txn.append_event(event)
    else:
        model = Model(
            version = old_model.version + 1,
            updated_at = time.time(),
            **command.dict(include=ModelDef.__fields__.keys(), exclude_unset=True),
            **old_model.dict(include=set(ModelInfo.__fields__.keys()).difference({'version', 'updated_at'}))
        )
        event = ModelCreated(
            domain = 'Modle',
            domain_id = model.name,
            **model.dict()
        )
        with repo.transaction() as txn:
            txn.update_model(model, on_version=old_model.version)
            txn.append_event(event)
