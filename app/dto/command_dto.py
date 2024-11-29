from typing import Dict, Any
from pydantic import BaseModel, Field

from app.api.v1.enums import CommandType


class CommandDTO(BaseModel):
    command_type: CommandType
    parameters: Dict[str, Any]
    priority: int = Field(default=1, ge=1, le=5)
    timeout: int = Field(default=30, ge=5, le=300)