from pydantic import BaseModel, ConfigDict
from typing import Any, List, Optional
from .instruction import Instruction
from .model_data import ModelData

class GridModel(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    baseMVA: Optional[float] = None
    Comments: Optional[str] = None
    ModelVersion: Optional[int] = None
    UserName: Optional[str] = None
    sender_id: Optional[str] = None
    instruction: Optional[Instruction] = None
    model_data: ModelData
    diagrams: List[Any] = []
