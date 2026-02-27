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
