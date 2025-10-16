from pydantic import BaseModel, ConfigDict, Field
from typing import Optional

class Circuit(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str
    comments: Optional[str] = None
    model_version: Optional[int] = Field(None, alias="model_version")
    user_name: Optional[str] = None
    Sbase: Optional[float] = None
    fBase: Optional[float] = None
    idtag: Optional[str] = None
