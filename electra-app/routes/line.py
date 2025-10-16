from fastapi import APIRouter
from services.line import get_line

router = APIRouter()

@router.get("/{line_id}")
def read_line(line_id: int):
    return get_line(line_id)
