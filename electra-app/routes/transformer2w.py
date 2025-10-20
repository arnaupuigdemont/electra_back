from fastapi import APIRouter
from services.transformer2w import get_transformer2w, list_transformers2w

router = APIRouter()

@router.get("/{transformer_id}")
def read_transformer2w(transformer_id: int):
    return get_transformer2w(transformer_id)

@router.get("/")
def read_transformers2w():
    return list_transformers2w()
