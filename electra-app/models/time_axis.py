from pydantic import BaseModel, ConfigDict
from typing import List, Optional

class TimeAxis(BaseModel):
    model_config = ConfigDict(extra="ignore")
    unix: List[int] = []
    prob: List[float] = []
    snapshot_unix: Optional[float] = None
