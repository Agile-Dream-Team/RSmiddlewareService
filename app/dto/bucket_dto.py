from typing import Optional
from pydantic import BaseModel


class BucketDTO(BaseModel):
    name: Optional[str] = None
    serial: Optional[str] = None
    description: Optional[str] = None
    device_id: Optional[int] = None
    user_id: Optional[int] = None


class BucketListDTO(BaseModel):
    buckets: list[BucketDTO]


class BucketRequestDTO(BaseModel):
    id: int
