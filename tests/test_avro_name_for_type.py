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
    assert _avro_name_for_type(list[ClassA]) == "TestAvroNameForTypeClassAList"


def test_list_of_union():
    assert _avro_name_for_type(list[str | int]) == "IntOrStrList"


def test_list_of_union_two_custom_classes():
    assert _avro_name_for_type(list[ClassA | ClassB]) == "TestAvroNameForTypeClassAOrTestAvroNameForTypeClassBList"


def test_list_of_optional_custom_class():
    assert _avro_name_for_type(list[ClassA | None]) == "NullOrTestAvroNameForTypeClassAList"


def test_list_of_dict_with_custom_class():
    assert _avro_name_for_type(list[dict[str, ClassA]]) == "TestAvroNameForTypeClassAMapList"


# Sets


def test_set_str():
    assert _avro_name_for_type(set[str]) == "StrSet"


def test_set_custom_class():
    assert _avro_name_for_type(set[ClassA]) == "TestAvroNameForTypeClassASet"


# Maps


def test_dict_str_str():
    assert _avro_name_for_type(dict[str, str]) == "StrMap"


def test_dict_custom_class_value():
    assert _avro_name_for_type(dict[str, ClassA]) == "TestAvroNameForTypeClassAMap"


def test_dict_with_union_two_custom_classes():
    assert _avro_name_for_type(dict[str, ClassA | ClassB]) == "TestAvroNameForTypeClassAOrTestAvroNameForTypeClassBMap"


def test_dict_with_optional_custom_class():
    assert _avro_name_for_type(dict[str, ClassA | None]) == "NullOrTestAvroNameForTypeClassAMap"


def test_dict_with_list_of_custom_class():
    assert _avro_name_for_type(dict[str, list[ClassA]]) == "TestAvroNameForTypeClassAListMap"


def test_dict_none_value():
    assert _avro_name_for_type(dict[str, None]) == "NullMap"


# Unions


def test_union_str_int():
    assert _avro_name_for_type(str | int) == "IntOrStr"


def test_union_str_int_legacy_syntax():
    assert _avro_name_for_type(typing.Union[str, int]) == "IntOrStr"


def test_union_two_custom_classes_order_independent():
    assert _avro_name_for_type(ClassA | ClassB) == "TestAvroNameForTypeClassAOrTestAvroNameForTypeClassB"
    assert _avro_name_for_type(ClassB | ClassA) == "TestAvroNameForTypeClassAOrTestAvroNameForTypeClassB"


def test_optional_custom_class():
    assert _avro_name_for_type(ClassA | None) == "NullOrTestAvroNameForTypeClassA"


# Special cases


def test_same_class_name_different_modules():

    ClassFromA = type("MyClass", (), {"__module__": "pkg.mod_a"})
    ClassFromB = type("MyClass", (), {"__module__": "pkg.mod_b"})

    name_a = _avro_name_for_type(list[ClassFromA])  # noqa
    name_b = _avro_name_for_type(list[ClassFromB])  # noqa

    assert name_a == "PkgModAMyClassList"
    assert name_b == "PkgModBMyClassList"


# Error cases


def test_unknown_type_raises():
    T = typing.TypeVar("T")
    with pytest.raises(TypeNotSupportedError, match="Cannot generate a wrapper record name"):
        _avro_name_for_type(T)
