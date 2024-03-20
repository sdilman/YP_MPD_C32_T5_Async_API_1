from pydantic import BaseModel, Field
from datetime import datetime
from uuid import UUID, uuid4
import orjson


class UUIDMixin(BaseModel):
    id: UUID = Field(primary_key=True, default_factory=uuid4)


def orjson_dumps(v, *, default):
    return orjson.dumps(v, default=default).decode()


class ConfigBase:
    json_loads = orjson.loads
    json_dumps = orjson_dumps 