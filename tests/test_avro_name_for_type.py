"""
Set of unit tests for the _avro_name_for_type function, as this is a pretty crucial component of our design with
wrapped records.
"""

import typing

import pytest

from py_avro_schema._schemas import TypeNotSupportedError, _avro_name_for_type


class ClassA:
    pass


class ClassB:
    pass


# Sequences


def test_list_str():
    assert _avro_name_for_type(list[str]) == "StrList"


def test_nested_list():
    assert _avro_name_for_type(list[list[str]]) == "StrListList"


def test_list_of_custom_class():
    assert _avro_name_for_type(list[ClassA]) == "ClassAList"


def test_list_of_union():
    assert _avro_name_for_type(list[str | int]) == "IntOrStrList"


def test_list_of_union_two_custom_classes():
    assert _avro_name_for_type(list[ClassA | ClassB]) == "ClassAOrClassBList"


def test_list_of_optional_custom_class():
    assert _avro_name_for_type(list[ClassA | None]) == "ClassAOrNullList"


def test_list_of_dict_with_custom_class():
    assert _avro_name_for_type(list[dict[str, ClassA]]) == "ClassAMapList"


# Sets


def test_set_str():
    assert _avro_name_for_type(set[str]) == "StrSet"


def test_set_custom_class():
    assert _avro_name_for_type(set[ClassA]) == "ClassASet"


# Maps


def test_dict_str_str():
    assert _avro_name_for_type(dict[str, str]) == "StrMap"


def test_dict_custom_class_value():
    assert _avro_name_for_type(dict[str, ClassA]) == "ClassAMap"


def test_dict_with_union_two_custom_classes():
    assert _avro_name_for_type(dict[str, ClassA | ClassB]) == "ClassAOrClassBMap"


def test_dict_with_optional_custom_class():
    assert _avro_name_for_type(dict[str, ClassA | None]) == "ClassAOrNullMap"


def test_dict_with_list_of_custom_class():
    assert _avro_name_for_type(dict[str, list[ClassA]]) == "ClassAListMap"


def test_dict_none_value():
    assert _avro_name_for_type(dict[str, None]) == "NullMap"


# Unions


def test_union_str_int():
    assert _avro_name_for_type(str | int) == "IntOrStr"


def test_union_str_int_legacy_syntax():
    assert _avro_name_for_type(typing.Union[str, int]) == "IntOrStr"


def test_union_two_custom_classes_order_independent():
    assert _avro_name_for_type(ClassA | ClassB) == "ClassAOrClassB"
    assert _avro_name_for_type(ClassB | ClassA) == "ClassAOrClassB"


def test_optional_custom_class():
    assert _avro_name_for_type(ClassA | None) == "ClassAOrNull"


# Error cases


def test_unknown_type_raises():
    T = typing.TypeVar("T")
    with pytest.raises(TypeNotSupportedError, match="Cannot generate a wrapper record name"):
        _avro_name_for_type(T)
