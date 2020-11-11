from collections.abc import Mapping
from dataclasses import dataclass, fields, Field
from typing import (
    Optional, TypeVar, Generic, Dict, get_type_hints, List, Tuple
)
from typing_extensions import Protocol

from hologram import JsonSchemaMixin
from hologram.helpers import StrEnum

from dbt import deprecations
from dbt.contracts.util import Replaceable
from dbt.exceptions import CompilationException
from dbt.utils import deep_merge


from mashumaro import DataClassJSONMixin

class RelationType(StrEnum):
    Table = 'table'
    View = 'view'
    CTE = 'cte'
    MaterializedView = 'materializedview'
    External = 'external'


class ComponentName(StrEnum):
    Database = 'database'
    Schema = 'schema'
    Identifier = 'identifier'


class HasQuoting(Protocol):
    quoting: Dict[str, bool]

"""
This is a throwaway shim to match the JsonSchemaMixin interface
that downstream consumers (ie. PostgresRelation) is expecting. 
I imagine that we would try to remove code that depends on this type
reflection if we pursue an approach like the one shown here
"""
class FastDataClassJSONMixing(DataClassJSONMixin):
    @classmethod
    def field_mapping(cls) -> Dict[str, str]:
        """Defines the mapping of python field names to JSON field names.

        The main use-case is to allow JSON field names which are Python keywords
        """
        return {}

    @classmethod
    def _get_fields(cls) -> List[Tuple[Field, str]]:
        mapped_fields = []
        type_hints = get_type_hints(cls)

        for f in fields(cls):
            # Skip internal fields
            if f.name.startswith("_"):
                continue

            # Note fields() doesn't resolve forward refs
            f.type = type_hints[f.name]

            mapped_fields.append((f, cls.field_mapping().get(f.name, f.name)))

        return mapped_fields  # type: ignore


class FakeAPIObject(FastDataClassJSONMixing, Replaceable, Mapping):
    # override the mapping truthiness, len is always >1
    def __bool__(self):
        return True

    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key) from None

    def __iter__(self):
        deprecations.warn('not-a-dictionary', obj=self)
        for _, name in self._get_fields():
            yield name

    def __len__(self):
        deprecations.warn('not-a-dictionary', obj=self)
        return len(fields(self.__class__))

    def incorporate(self, **kwargs):
        value = self.to_dict()
        value = deep_merge(value, kwargs)
        return self.from_dict(value)


T = TypeVar('T')


@dataclass
class Policy(FakeAPIObject):
    database: bool = True
    schema: bool = True
    identifier: bool = True

    def get_part(self, key: ComponentName) -> bool:
        if key == ComponentName.Database:
            return self.database
        elif key == ComponentName.Schema:
            return self.schema
        elif key == ComponentName.Identifier:
            return self.identifier
        else:
            raise ValueError(
                'Got a key of {}, expected one of {}'
                .format(key, list(ComponentName))
            )

    def replace_dict(self, dct: Dict[ComponentName, bool]):
        kwargs: Dict[str, bool] = {}
        for k, v in dct.items():
            kwargs[str(k)] = v
        return self.replace(**kwargs)


@dataclass
class Path(FakeAPIObject):
    database: Optional[str]
    schema: Optional[str]
    identifier: Optional[str]

    def __post_init__(self):
        # handle pesky jinja2.Undefined sneaking in here and messing up rende
        if not isinstance(self.database, (type(None), str)):
            raise CompilationException(
                'Got an invalid path database: {}'.format(self.database)
            )
        if not isinstance(self.schema, (type(None), str)):
            raise CompilationException(
                'Got an invalid path schema: {}'.format(self.schema)
            )
        if not isinstance(self.identifier, (type(None), str)):
            raise CompilationException(
                'Got an invalid path identifier: {}'.format(self.identifier)
            )

    def get_lowered_part(self, key: ComponentName) -> Optional[str]:
        part = self.get_part(key)
        if part is not None:
            part = part.lower()
        return part

    def get_part(self, key: ComponentName) -> str:
        if key == ComponentName.Database:
            return self.database
        elif key == ComponentName.Schema:
            return self.schema
        elif key == ComponentName.Identifier:
            return self.identifier
        else:
            raise ValueError(
                'Got a key of {}, expected one of {}'
                .format(key, list(ComponentName))
            )

    def replace_dict(self, dct: Dict[ComponentName, str]):
        kwargs: Dict[str, str] = {}
        for k, v in dct.items():
            kwargs[str(k)] = v
        return self.replace(**kwargs)
