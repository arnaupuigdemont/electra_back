from pydantic import BaseModel, ConfigDict
from typing import Optional

class Instruction(BaseModel):
    model_config = ConfigDict(extra="ignore")
    operation: Optional[str] = None
    user: Optional[str] = None
    mac: Optional[str] = None
