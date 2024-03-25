from pydantic import BaseModel, Field
from datetime import datetime
from uuid import UUID, uuid4
import orjson


class UUIDMixin(BaseModel):
    uuid: UUID = Field(primary_key=True, default_factory=uuid4, alias='id')
