
from dataclasses import dataclass, fields, Field
from typing import (
    Optional, TypeVar, Generic, Dict, get_type_hints, List, Tuple
)
from mashumaro import DataClassDictMixin
from mashumaro.types import SerializableType
import re


"""
This is a throwaway shim to match the JsonSchemaMixin interface

that downstream consumers (ie. PostgresRelation) is expecting. 
I imagine that we would try to remove code that depends on this type
reflection if we pursue an approach like the one shown here
"""
class dbtClassMixin(DataClassDictMixin):
    @classmethod
    def field_mapping(cls) -> Dict[str, str]:
        """Defines the mapping of python field names to JSON field names.

        The main use-case is to allow JSON field names which are Python keywords
        """
        return {}

    def serialize(self, omit_none=False, validate=False, with_aliases: Optional[Dict[str, str]]=None):
        dct = self.to_dict()

        if with_aliases:
            # TODO : Mutating these dicts is a TERRIBLE idea - remove this
            for aliased_name, canonical_name in self._ALIASES.items():
                if aliased_name in dct:
                    dct[canonical_name] = dct.pop(aliased_name)

        return dct

    @classmethod
    def deserialize(cls, data, validate=False, with_aliases=False):
        if with_aliases:
            # TODO : Mutating these dicts is a TERRIBLE idea - remove this
            for aliased_name, canonical_name in cls._ALIASES.items():
                if aliased_name in data:
                    data[canonical_name] = data.pop(aliased_name)

        # TODO .... implement these?
        return cls.from_dict(data)


class ValidatedStringMixin(str, SerializableType):
    ValidationRegex = None

    @classmethod
    def _deserialize(cls, value: str) -> 'ValidatedStringMixin':
        cls.validate(value)
        return ValidatedStringMixin(value)

    def _serialize(self) -> str:
        return str(self)

    @classmethod
    def validate(cls, value) -> str:
        res = re.match(cls.ValidationRegex, value)

        if res is None:
            raise ValidationError(f"Invalid value: {value}") # TODO
