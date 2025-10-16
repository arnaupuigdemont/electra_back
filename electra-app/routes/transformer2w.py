from fastapi import APIRouter
from services.transformer2w import get_transformer2w

router = APIRouter()

@router.get("/{transformer_id}")
def read_transformer2w(transformer_id: int):
    return get_transformer2w(transformer_id)
