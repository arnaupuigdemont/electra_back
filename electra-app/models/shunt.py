from pydantic import BaseModel, ConfigDict
from typing import Optional

class Shunt(BaseModel):
    model_config = ConfigDict(extra="ignore")
    grid_id: int | None = None
    idtag: str
    name: str
    code: str
    rdfid: str
    action: str
    comment: str
    modelling_authority: str
    commissioned_date: int
    decommissioned_date: int
    bus: str
    active: bool
    B: Optional[float] = None
