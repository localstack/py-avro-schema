"""
Module to register aliases for Python types

This module maintains global state via the _ALIASES registry.
Decorators will modify this state when applied to classes.
"""

from collections import defaultdict

FQN = str
"""Fully qualified name for a Python type"""
_ALIASES: dict[FQN, set[FQN]] = defaultdict(set)
"""Maps the FQN of a Python type to a set of aliases"""


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
