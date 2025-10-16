from fastapi import APIRouter
from services.generator import get_generator

router = APIRouter()

@router.get("/{generator_id}")
def read_generator(generator_id: int):
    return get_generator(generator_id)
