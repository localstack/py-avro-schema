# Copyright 2022 J.P. Morgan Chase & Co.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

from enum import StrEnum
from typing import Annotated, NotRequired, TypedDict, Union

import pytest

import py_avro_schema
import py_avro_schema as pas
from py_avro_schema._alias import Alias, register_type_alias
from py_avro_schema._testing import assert_schema


def test_typed_dict():
    class User(TypedDict):
        name: str
        age: int

    expected = {
        "type": "record",
        "name": "User",
        "fields": [
            {
                "name": "name",
                "type": "string",
            },
            {"name": "age", "type": "long"},
        ],
    }

    assert_schema(User, expected)

    User = TypedDict("User", {"name": str, "age": int})
    assert_schema(User, expected)


def test_type_dict_nested():
    @register_type_alias("test_typed_dict.OldAddress")
    class Address(TypedDict):
        street: Annotated[str, Alias("address")]
        number: int

    class User(TypedDict):
        name: str
        age: int
        address: Address

    expected = {
        "type": "record",
        "name": "User",
        "namespace": "test_typed_dict",
        "fields": [
            {
                "name": "name",
                "type": "string",
            },
            {"name": "age", "type": "long"},
            {
                "name": "address",
                "type": {
                    "name": "Address",
                    "namespace": "test_typed_dict",
                    "aliases": ["test_typed_dict.OldAddress"],
                    "type": "record",
                    "fields": [
                        {"aliases": ["address"], "name": "street", "type": "string"},
                        {"name": "number", "type": "long"},
                    ],
                },
            },
        ],
    }
    assert_schema(User, expected, do_auto_namespace=True)


def test_field_alias():
    class User(TypedDict):
        name: Annotated[str, Alias("username")]
        age: int

    expected = {
        "type": "record",
        "name": "User",
        "fields": [
            {
                "aliases": ["username"],
                "name": "name",
                "type": "string",
            },
            {"name": "age", "type": "long"},
        ],
    }

    assert_schema(User, expected)


def test_non_total_typed_dict():
    class InvalidEnumSymbol(StrEnum):
        val = "invalid-val"

    class ValidEnumSymbol(StrEnum):
        val = "valid_val"

    class PyType(TypedDict, total=False):
        name: str
        nickname: str | None
        age: int | None
        invalid: InvalidEnumSymbol | None
        valid: ValidEnumSymbol | None
        bytes_data: bytes
        bytes_data_nullable: bytes | None

    expected = {
        "fields": [
            {"name": "name", "type": {"namedString": "TDMissingMarker", "type": "string"}},
            {
                "default": "__td_missing__",
                "name": "nickname",
                "type": ["null", {"namedString": "TDMissingMarker", "type": "string"}],
            },
            {
                "default": "__td_missing__",
                "name": "age",
                "type": [{"namedString": "TDMissingMarker", "type": "string"}, "long", "null"],
            },
            {
                "default": "__td_missing__",
                "name": "invalid",
                "type": [{"namedString": "TDMissingMarker", "type": "string"}, "null"],
            },
            {
                "default": "__td_missing__",
                "name": "valid",
                "type": [
                    {"namedString": "TDMissingMarker", "type": "string"},
                    {"default": "valid_val", "name": "ValidEnumSymbol", "symbols": ["valid_val"], "type": "enum"},
                    "null",
                ],
            },
            {
                "name": "bytes_data",
                "type": "bytes",
            },
            {"default": "__td_missing__", "name": "bytes_data_nullable", "type": ["bytes", "null"]},
        ],
        "name": "PyType",
        "type": "record",
    }
    assert_schema(PyType, expected, options=pas.Option.MARK_NON_TOTAL_TYPED_DICTS)


def test_non_required_keyword():
    class PyType(TypedDict):
        name: str
        value: NotRequired[str]
        value_int: NotRequired[int]
        nullable_value: NotRequired[str | None]

    expected = {
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "value", "type": {"namedString": "TDMissingMarker", "type": "string"}},
            {"name": "value_int", "type": ["long", {"namedString": "TDMissingMarker", "type": "string"}]},
            {"name": "nullable_value", "type": ["null", {"namedString": "TDMissingMarker", "type": "string"}]},
        ],
        "name": "PyType",
        "type": "record",
    }

    assert_schema(PyType, expected, options=pas.Option.MARK_NON_TOTAL_TYPED_DICTS)


def test_reference_id():
    class PyType(TypedDict):
        var: str

    expected = {
        "type": "record",
        "name": "PyType",
        "fields": [
            {
                "name": "var",
                "type": "string",
            },
            {
                "default": None,
                "name": "__id",
                "type": ["null", "long"],
            },
        ],
    }
    assert_schema(PyType, expected, options=pas.Option.ADD_REFERENCE_ID)


def test_union_typed_dict_error():
    class PyType(TypedDict):
        var: str

    class PyType2(TypedDict):
        var: str

    py_type = Union[PyType, PyType2]
    with pytest.raises(TypeError):
        py_avro_schema._schemas.schema(py_type)


class SiblingInner(TypedDict):
    x: str


class SiblingOuter(TypedDict):
    a: SiblingInner
    b: list[SiblingInner]


def test_sibling_fields_same_():
    expected = {
        "type": "record",
        "name": "SiblingOuter",
        "namespace": "test_typed_dict",
        "fields": [
            {
                "name": "a",
                "type": {
                    "type": "record",
                    "name": "SiblingInner",
                    "namespace": "test_typed_dict",
                    "fields": [{"name": "x", "type": "string"}],
                },
            },
            {
                "name": "b",
                "type": {
                    "type": "record",
                    "name": "TestTypedDictSiblingInnerList",
                    "namespace": "builtins",
                    "fields": [
                        {"name": "__id", "type": ["null", "long"], "default": None},
                        {
                            "name": "__data",
                            "type": {
                                "type": "array",
                                "items": "test_typed_dict.SiblingInner",
                            },
                        },
                    ],
                },
            },
        ],
    }
    assert_schema(
        SiblingOuter,
        expected,
        options=pas.Option.WRAP_INTO_RECORDS,
        do_auto_namespace=True,
    )


ConfigurationList = list["Configuration"]


class Configuration(TypedDict):
    Configurations: ConfigurationList | None


def test_recursive_reference():
    """Test simple recursive reference with no ``WRAP_INTO_RECORDS``."""

    class PyType(TypedDict):
        Configurations: ConfigurationList | None

    expected = {
        "type": "record",
        "name": "PyType",
        "fields": [
            {
                "name": "Configurations",
                "type": [
                    {
                        "type": "array",
                        "items": {
                            "type": "record",
                            "name": "Configuration",
                            "fields": [
                                {
                                    "name": "Configurations",
                                    "type": [
                                        {"type": "array", "items": "Configuration"},
                                        "null",
                                    ],
                                },
                            ],
                        },
                    },
                    "null",
                ],
            },
        ],
    }
    assert_schema(PyType, expected)


def test_recursive_reference_with_wrap_into_records():
    """Checks that a self-referential record combined with ``WRAP_INTO_RECORDS`` must define the wrapper once."""

    class PyType(TypedDict):
        Configurations: ConfigurationList | None

    expected = {
        "type": "record",
        "name": "PyType",
        "fields": [
            {
                "name": "Configurations",
                "type": [
                    {
                        "type": "record",
                        "name": "TestTypedDictConfigurationList",
                        "fields": [
                            {"name": "__id", "type": ["null", "long"], "default": None},
                            {
                                "name": "__data",
                                "type": {
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "Configuration",
                                        "fields": [
                                            {
                                                "name": "Configurations",
                                                "type": [
                                                    "TestTypedDictConfigurationList",
                                                    "null",
                                                ],
                                            },
                                        ],
                                    },
                                },
                            },
                        ],
                    },
                    "null",
                ],
            },
        ],
    }
    assert_schema(PyType, expected, options=pas.Option.WRAP_INTO_RECORDS)
