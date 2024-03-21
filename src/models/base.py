from pydantic import BaseModel, Field
from datetime import datetime
from uuid import UUID, uuid4
import orjson


class UUIDMixin(BaseModel):
    id: UUID = Field(primary_key=True, default_factory=uuid4)
