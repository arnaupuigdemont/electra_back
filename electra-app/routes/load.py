from fastapi import APIRouter
from pydantic import BaseModel
from services.load import get_load, list_loads, update_load_status

router = APIRouter()


class StatusUpdate(BaseModel):
    active: bool


@router.get("/{load_id}")
def read_load(load_id: int):
    return get_load(load_id)

@router.get("/")
def read_loads():
    return list_loads()

@router.patch("/{load_id}/status")
def update_status(load_id: int, status_update: StatusUpdate):
    return update_load_status(load_id, status_update.active)
