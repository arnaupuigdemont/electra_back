from pydantic import BaseModel

class ImportResult(BaseModel):
    inserted_buses: int
    status: str
