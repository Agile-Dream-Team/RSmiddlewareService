from app.api.v1.enums import DeviceCapability
from pydantic import BaseModel
from typing import List, Dict


class ESP32DTO(BaseModel):
    esp_id: str
    name: str
    capabilities: List[DeviceCapability]
    location: str
    metadata: Dict = {}
