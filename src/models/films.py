from pydantic import BaseModel


# Модель ответа API
class Film(BaseModel):
    id: str
    title: str