from fastapi import APIRouter
from pydantic import BaseModel
from services.line import get_line, list_lines, update_line_status

router = APIRouter()


class StatusUpdate(BaseModel):
    active: bool


@router.get("/{line_id}")
def read_line(line_id: int):
    return get_line(line_id)

@router.get("/")
def read_lines():
    return list_lines()

@router.patch("/{line_id}/status")
def update_status(line_id: int, status_update: StatusUpdate):
    return update_line_status(line_id, status_update.active)
