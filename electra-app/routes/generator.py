from fastapi import APIRouter
from pydantic import BaseModel
from services.generator import get_generator, list_generators, update_generator_status

router = APIRouter()


class StatusUpdate(BaseModel):
    active: bool


@router.get("/{generator_id}")
def read_generator(generator_id: int):
    return get_generator(generator_id)

@router.get("/")
def read_generators():
    return list_generators()

@router.patch("/{generator_id}/status")
def update_status(generator_id: int, status_update: StatusUpdate):
    return update_generator_status(generator_id, status_update.active)
