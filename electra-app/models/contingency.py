from pydantic import BaseModel, ConfigDict
from typing import Any, Optional

class ContingencyGroup(BaseModel):
    model_config = ConfigDict(extra="ignore")
    idtag: str
    name: str
    code: str
    rdfid: str
    action: str
    comment: str
    category: Optional[str] = None

class Contingency(BaseModel):
    model_config = ConfigDict(extra="ignore")
    idtag: str
    name: str
    code: str
    rdfid: str
    action: str
    comment: str
    device_idtag: str
    tpe: str
    prop: str
    value: Any
    group: Optional[str] = None
