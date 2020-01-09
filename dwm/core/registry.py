from typing import (
    Any,
    Iterable,
    Tuple,
    Mapping,
    TypeVar,
    Generic,
)
from types import MappingProxyType


K = TypeVar("K")
V = TypeVar("V")


class DuplicatedRegistryKey(Exception):
    pass


class Registry(Generic[K, V]):
    def __init__(self):
        self.__registry = {}

    def _transform_key(self, key: K) -> Any:
        return key

    def _transform_value(self, value: V) -> Any:
        return value

    def __delitem__(self, key: K):
        return self.__registry.__delitem__(self._transform_key(key))

    def __getitem__(self, key: K):
        return self.__registry.__getitem__(self._transform_key(key))

    def __contains__(self, key: K):
        return self.__registry.__contains__(self._transform_key(key))

    @property
    def entries(self):
        return MappingProxyType(self.__registry)

    def merge(self, entries: Mapping[K, V], check_dup: bool = False):
        if check_dup:
            dup_keys = set(self.__entries.keys()).intersection(entries.keys())
            if dup_keys:
                raise DuplicatedRegistryKey(tuple(dup_keys))
        self.__registry.update(entries)
        return self

    def copy(self, include: Iterable[K] = None, exclude: Iterable[K] = None):
        keys = set(self.__registry.keys())
        if include:
            keys = keys.intersection(self._transform_key(k) for k in include)
        if exclude:
            keys = keys.difference(self._transform_key(k) for k in exclude)
        return self.__class__().merge({k: self.__registry[k] for k in keys})

    def __add__(self, other: 'Registry'):
        if other.__class__ is not self.__class__:
            raise TypeError("addition only happens between smae type of registry")
        return self.copy().merge(other.entries, True)


class DecoRegistry(Registry[K, V]):
    def __call__(self, key: K, check_dup: bool = True):
        def deco(value: V):
            self.merge({self._transform_key(key): self._transform_value(value)})
            return value

        return deco
