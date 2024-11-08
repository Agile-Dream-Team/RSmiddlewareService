from typing import Optional, List
from pydantic import BaseModel
from app.dto.bucket_dto import BucketDTO


class DeviceDTO(BaseModel):
    pod_id: Optional[int] = None
    serial: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    user_id: Optional[int] = None


class DeviceListDTO(BaseModel):
    devices: list[DeviceDTO]


class DeviceWithBucketsDTO(DeviceDTO):
    buckets: List[BucketDTO] = []


class DeviceRequestDTO(BaseModel):
    id: int


class DeviceBucketsRequestDTO(BaseModel):
    device_id: int
