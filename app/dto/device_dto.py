from typing import Optional, List, Dict
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


class ESP32DeviceDTO(BaseModel):
    esp_id: str
    name: str
    location: str
    capabilities: List[str]
    device_metadata: Optional[Dict] = {}


class DeviceRegistrationDTO(BaseModel):
    name: str
    serial: str
    device_type: str = "raspberry_pi"
    status: str = "active"
    ip_address: Optional[str]
    port: Optional[int]
    location: Optional[str]
    device_metadata: Optional[Dict] = {}
    pod_id: Optional[int]
    user_id: Optional[int]
    esp_devices: Optional[List[ESP32DeviceDTO]] = []


class ESP32StatusUpdateDTO(BaseModel):
    esp_id: str
    status: str
    last_seen: Optional[str] = None
    device_metadata: Optional[Dict] = {}


class BulkESP32StatusUpdateDTO(BaseModel):
    updates: List[ESP32StatusUpdateDTO]


class DeviceHeartbeatDTO(BaseModel):
    device_id: int
    status: str = "active"
    esp_devices: Optional[List[ESP32StatusUpdateDTO]] = []
