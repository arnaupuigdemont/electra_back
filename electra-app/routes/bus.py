from fastapi import APIRouter
from services import bus

router = APIRouter()


@router.get("/{bus_id}")
def get_bus(bus_id: int):
    return bus.get_bus(bus_id)
