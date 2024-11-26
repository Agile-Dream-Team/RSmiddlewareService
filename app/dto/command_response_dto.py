from typing import List, Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field


class CommandResponseDTO(BaseModel):
    success: bool
    message: str
    data: Optional[Dict] = None
    timestamp: datetime = Field(default_factory=datetime.now)
