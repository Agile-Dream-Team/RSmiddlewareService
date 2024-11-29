from typing import List, Optional, Dict
from pydantic import BaseModel


class DeviceMapper(BaseModel):
    pod_id: int
    serial: str
    name: str
    description: str
    user_id: int | None = None


class UpdateDeviceMapper(BaseModel):
    id: int
    pod_id: int | None = None
    serial: str | None = None
    name: str | None = None
    description: str | None = None
    user_id: int | None = None

    def update_from_dict(self, data: dict):
        for field, value in data.items():
            if hasattr(self, field) and value is not None:
                setattr(self, field, value)
        return self

    def model_dump_not_none(self):
        return {k: v for k, v in self.model_dump().items() if v is not None}


class ESP32DeviceMapper(BaseModel):
    esp_id: str
    name: str
    location: str
    capabilities: List[str]
    device_metadata: Optional[Dict] = {}


class DeviceRegistrationMapper(BaseModel):
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
    esp_devices: Optional[List[ESP32DeviceMapper]] = []

    def model_dump_not_none(self):
        return {k: v for k, v in self.model_dump().items() if v is not None}
