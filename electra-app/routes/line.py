from fastapi import APIRouter
from services.line import get_line, list_lines

router = APIRouter()

@router.get("/{line_id}")
def read_line(line_id: int):
    return get_line(line_id)

@router.get("/")
def read_lines():
    return list_lines()
