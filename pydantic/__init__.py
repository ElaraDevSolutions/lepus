# Minimal fallback stub for pydantic BaseModel (for environments without real pydantic)
# This is NOT full-featured; only supports simple type checking for int, float, str, bool.
from typing import Any, Dict

class ValidationError(Exception):
    pass

class BaseModel:
    def __init__(self, **data):
        annotations = getattr(self.__class__, '__annotations__', {})
        for field, typ in annotations.items():
            if field not in data:
                raise ValueError(f"Missing field: {field}")
            val = data[field]
            if typ in (int, float, str, bool) and not isinstance(val, typ):
                raise ValidationError(f"Field {field} expected {typ.__name__}, got {type(val).__name__}")
            setattr(self, field, val)
        # allow extra fields silently
        for k,v in data.items():
            if k not in annotations:
                setattr(self, k, v)
    @classmethod
    def parse_obj(cls, obj: Dict[str, Any]):
        if not isinstance(obj, dict):
            raise TypeError("parse_obj expects dict")
        return cls(**obj)
    def dict(self):
        return {k: getattr(self, k) for k in getattr(self.__class__, '__annotations__', {}).keys()}

__all__ = ['BaseModel','ValidationError']
