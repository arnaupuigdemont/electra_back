from fastapi import APIRouter
from services.shunt import get_shunt, list_shunts

router = APIRouter()

@router.get("/{shunt_id}")
def read_shunt(shunt_id: int):
    return get_shunt(shunt_id)

@router.get("/")
def read_shunts():
    return list_shunts()
