from fastapi import APIRouter
from pydantic import BaseModel
from services.shunt import get_shunt, list_shunts, update_shunt_status

router = APIRouter()


class StatusUpdate(BaseModel):
    active: bool


@router.get("/{shunt_id}")
def read_shunt(shunt_id: int):
    return get_shunt(shunt_id)

@router.get("/")
def read_shunts():
    return list_shunts()

@router.patch("/{shunt_id}/status")
def update_status(shunt_id: int, status_update: StatusUpdate):
    return update_shunt_status(shunt_id, status_update.active)
