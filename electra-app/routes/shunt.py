from fastapi import APIRouter
from services.shunt import get_shunt

router = APIRouter()

@router.get("/{shunt_id}")
def read_shunt(shunt_id: int):
    return get_shunt(shunt_id)
