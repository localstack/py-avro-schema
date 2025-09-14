from typing import Annotated, TypedDict

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
