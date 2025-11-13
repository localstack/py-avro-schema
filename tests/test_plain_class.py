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

import re
from typing import Annotated, Final

import pytest

import py_avro_schema
from py_avro_schema._alias import Alias, Opaque, register_type_aliases
from py_avro_schema._testing import assert_schema


def test_plain_class_with_type_hints():
    @register_type_aliases(aliases=["test_plain_class.OldPyType"])
    class PyType:
        """A port, not using dataclass"""

        name: str
        country: str
        latitude: float
        longitude: float

        def __init__(
            self, name: str, *, country: str = "NLD", latitude: float, longitude: float
        ):  # note the non-default kwargs near the end!
            self.name = name
            self.country = country.upper()
            self.latitude = latitude
            self.longitude = longitude

    expected = {
        "type": "record",
        "name": "PyType",
        "namespace": "test_plain_class",
        "aliases": ["test_plain_class.OldPyType"],
        "fields": [
            {
                "name": "name",
                "type": "string",
            },
            {
                "name": "country",
                "type": "string",
                "default": "NLD",
            },
            {
                "name": "latitude",
                "type": "double",
            },
            {
                "name": "longitude",
                "type": "double",
            },
        ],
    }

    assert_schema(PyType, expected, do_auto_namespace=True)


def test_plain_class_annotated():
    class PyType:
        """A port, not using dataclass"""

        name: str

        def __init__(self, name: str):
            self.name = name

    expected = {
        "type": "record",
        "name": "PyType",
        "fields": [
            {
                "name": "name",
                "type": "string",
            },
        ],
    }

    assert_schema(Annotated[PyType, ...], expected)


def test_field_alias():
    class PyType:
        name: Annotated[str, Alias("old_name")]

        def __init__(self, name):
            self.name = name

    expected = {
        "type": "record",
        "name": "PyType",
        "fields": [
            {
                "aliases": ["old_name"],
                "name": "name",
                "type": "string",
            },
        ],
    }
    assert_schema(PyType, expected)


def test_plain_class_no_type_hints():
    class PyType:
        """A port, not using dataclass"""

        def __init__(
            self, name: str, *, country: str = "NLD", latitude: float, longitude: float
        ):  # note the non-default kwargs near the end!
            self.name = name
            self.country = country.upper()
            self.latitude = latitude
            self.longitude = longitude

    with pytest.raises(
        py_avro_schema.TypeNotSupportedError,
        match=re.escape(
            "Cannot generate Avro schema for Python type <class 'test_plain_class.test_plain_class_no_type_hints."
            "<locals>.PyType'>"
        ),
    ):
        assert_schema(PyType, {})


def test_opaque_field():
    class Details:
        name: str
        surname: str
        age: int

    class PyType:
        details: Annotated[Details, Opaque]

    expected = {"fields": [{"name": "details", "type": "string"}], "name": "PyType", "type": "record"}
    assert_schema(PyType, expected)


def test_type_aliases():
    Name = str

    class PyClass:
        name: Name

    expected = {"fields": [{"name": "name", "type": "string"}], "name": "PyClass", "type": "record"}
    assert_schema(PyClass, expected)


def test_type_aliases_future():
    from tests.models.forward import PyClass

    expected = {"fields": [{"name": "name", "type": "string"}], "name": "PyClass", "type": "record"}
    assert_schema(PyClass, expected)


def test_typing_final():

    class PyType:
        var: Final[str]
        field: Final[dict[str, int]]

        def __init__(self):
            self.var = "Hello World"
            self.field = {"John": 123}

    expected = {
        "fields": [{"name": "var", "type": "string"}, {"name": "field", "type": {"type": "map", "values": "long"}}],
        "name": "PyType",
        "type": "record",
    }

    assert_schema(PyType, expected)
