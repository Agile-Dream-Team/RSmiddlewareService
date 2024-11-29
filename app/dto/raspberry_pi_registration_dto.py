from pydantic import BaseModel
from typing import List

from app.dto.esp32_dto import ESP32DTO


class RaspberryPiRegistrationDTO(BaseModel):
    device_id: str
    ip_address: str
    port: int
    name: str
    location: str
    esp_devices: List[ESP32DTO] = []
