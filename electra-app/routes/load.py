from fastapi import APIRouter
from services.load import get_load, list_loads

router = APIRouter()

@router.get("/{load_id}")
def read_load(load_id: int):
    return get_load(load_id)

@router.get("/")
def read_loads():
    return list_loads()
