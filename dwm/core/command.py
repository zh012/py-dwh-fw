from functools import partial
from inspect import signature
from typing import Callable, Any, Union, Type

from .data import DataClass
from .registry import Registry, DecoRegistry


class Command(DataClass):
    command_id: str

    class Config:
        allow_mutation = False


def get_command_name(command: Union[str, Command, Type[Command]]):
    if isinstance(command, str):
        return command
    if isinstance(command, Command):
        klass = command.__class__
    else:
        klass = command
    return getattr(klass, "__command_name__", klass.__name__)


class CommandContextBindingError(Exception):
    pass


class CommandContext(Registry[str, Any]):
    def bind(self, handler: Callable) -> Callable:
        params = list(signature(handler).parameters.keys())[1:]
        if not params:
            return handler
        try:
            kwargs = {p: self[p] for p in params}
        except KeyError as e:
            raise CommandContextBindingError(e.args[0])
        return partial(handler, **kwargs)


class CommandHandlerNotFound(Exception):
    pass


class CommandHandler(DecoRegistry[Type[Command], Callable[..., Any]]):
    def _transform_key(self, key):
        return get_command_name(key)

    def bind(self, command_context: CommandContext):
        return CommandHandler().merge(
            {k: command_context.bind(v) for k, v in self.entries.items()}
        )

    def execute(self, command: Command):
        command_name = get_command_name(command)
        if command_name not in self:
            raise CommandHandlerNotFound(command_name)
        self[command_name](command)
