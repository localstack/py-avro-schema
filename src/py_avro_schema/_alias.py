"""
Module to register aliases for Python types

This module maintains global state via the _ALIASES registry.
Decorators will modify this state when applied to classes.
"""

import dataclasses
from collections import defaultdict
from typing import Annotated, Type, get_args, get_origin

FQN = str
"""Fully qualified name for a Python type"""
_ALIASES: dict[FQN, set[FQN]] = defaultdict(set)
"""Maps the FQN of a Python type to a set of aliases"""


@dataclasses.dataclass
class Alias:
    """Alias for a record field"""

    alias: str


@dataclasses.dataclass
class Aliases:
    """Aliases for a record field"""

    aliases: list[str]


class Opaque:
    """
    This is a marker for complex Avro fields (e.g., maps) that are serialized to a simple string.
    """

    pass


def get_fully_qualified_name(py_type: type) -> str:
    """Returns the fully qualified name for a Python type"""
    module = getattr(py_type, "__module__", None)
    qualname = getattr(py_type, "__qualname__", py_type.__name__)

    # py-avro-schema does not consider <locals> in the namespace.
    # we skip it here as well for consistency
    if module and "<locals>" in qualname:
        return f"{module}.{py_type.__name__}"

    if module and module not in ("builtins", "__main__"):
        return f"{module}.{qualname}"
    return qualname


def register_type_aliases(aliases: list[FQN]):
    """
    Decorator to register aliases for a given type.
    It allows for compatible schemas following a change type (e.g., a rename), if the type fields do not
    change in an incompatible way.

    Example::
        @register_type_aliases(aliases=["py_avro_schema.OldAddress"])
        class Address(TypedDict):
            street: str
            number: int
    """

    def _wrapper(cls):
        """Wrapper function that updates the aliases dictionary"""
        fqn = get_fully_qualified_name(cls)
        _ALIASES[fqn].update(aliases)
        return cls

    return _wrapper


def register_type_alias(alias: FQN):
    """
    Decorator to register a single alias for a given type.
    It allows for compatible schemas following a change type (e.g., a rename), if the type fields do not
    change in an incompatible way.

    Example::
        @register_type_alias(alias="py_avro_schema.OldAddress")
        class Address(TypedDict):
            street: str
            number: int
    """

    def _wrapper(cls):
        """Wrapper function that updates the aliases dictionary"""
        fqn = get_fully_qualified_name(cls)
        _ALIASES[fqn].add(alias)
        return cls

    return _wrapper


def get_aliases(fqn: str) -> list[str]:
    """Returns the list of aliases for a given type"""
    if aliases := _ALIASES.get(fqn):
        return sorted(aliases)
    return []


def get_field_aliases_and_actual_type(py_type: Type) -> tuple[list[str] | None, Type]:
    """
    Check if a type contains an alias metadata via `Alias` or `Aliases` as metadata.
    It returns the eventual aliases and the type.
    """
    # py_type is not annotated. It can't have aliases
    if get_origin(py_type) is not Annotated:
        return [], py_type

    args = get_args(py_type)
    actual_type, annotation = args[0], args[1]

    # When a field is annotated with the Opaque class, we return bytes as type.
    #   The object serializer is responsible for dumping the entire attribute as a JSON string
    if isinstance(annotation, type) and issubclass(annotation, Opaque):
        return [], str

    # Annotated type but not an alias. We do nothing.
    if type(annotation) not in (Alias, Aliases):
        return [], py_type

    # If the annotated type is an alias, we extract the aliases and return the actual type
    aliases = annotation.aliases if type(annotation) is Aliases else [annotation.alias]
    return aliases, actual_type
