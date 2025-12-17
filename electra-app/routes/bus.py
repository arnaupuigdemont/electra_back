from fastapi import APIRouter
from pydantic import BaseModel
from services import bus

router = APIRouter()


class BusStatusUpdate(BaseModel):
    active: bool


@router.get("/")
def list_buses():
    return bus.list_buses()

@router.get("/{bus_id}")
def get_bus(bus_id: int):
    return bus.get_bus(bus_id)

@router.patch("/{bus_id}/status")
def update_bus_status(bus_id: int, status_update: BusStatusUpdate):
    return bus.update_bus_status(bus_id, status_update.active)
