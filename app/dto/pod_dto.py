from typing import Optional, List
from pydantic import BaseModel
from app.dto.device_dto import DeviceDTO


class PodDTO(BaseModel):
    name: str
    description: Optional[str] = None
    user_id: Optional[int] = None


class PodListDTO(BaseModel):
    pods: list[PodDTO]


class PodWithDevicesDTO(PodDTO):
    devices: List[DeviceDTO] = []


class PodRequestDTO(BaseModel):
    id: int


class PodDevicesRequestDTO(BaseModel):
    pod_id: int
