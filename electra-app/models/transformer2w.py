from pydantic import BaseModel, ConfigDict
from typing import Optional

class Transformer2W(BaseModel):
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
    bus_from: str
    bus_to: str
    active: bool
    R: Optional[float] = None
    X: Optional[float] = None
    G: Optional[float] = None
    B: Optional[float] = None
    HV: Optional[float] = None
    LV: Optional[float] = None
    Sn: Optional[float] = None
