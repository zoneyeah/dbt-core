
from dataclasses import dataclass, fields, Field
from typing import (
    Optional, TypeVar, Generic, Dict, get_type_hints, List, Tuple
)
from mashumaro import DataClassJSONMixin


"""
This is a throwaway shim to match the JsonSchemaMixin interface
that downstream consumers (ie. PostgresRelation) is expecting. 
I imagine that we would try to remove code that depends on this type
reflection if we pursue an approach like the one shown here
"""
class JsonSchemaMixin(DataClassJSONMixin):
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

