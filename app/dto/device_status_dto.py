from typing import Dict
from datetime import datetime
from pydantic import BaseModel

from app.api.v1.enums import DeviceStatus


class DeviceStatusDTO(BaseModel):
    status: DeviceStatus
    last_seen: datetime
    metadata: Dict = {}
