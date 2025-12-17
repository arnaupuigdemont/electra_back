from fastapi import APIRouter
from pydantic import BaseModel
from services.transformer2w import get_transformer2w, list_transformers2w, update_transformer_status

router = APIRouter()


class StatusUpdate(BaseModel):
    active: bool


@router.get("/{transformer_id}")
def read_transformer2w(transformer_id: int):
    return get_transformer2w(transformer_id)

@router.get("/")
def read_transformers2w():
    return list_transformers2w()

@router.patch("/{transformer_id}/status")
def update_status(transformer_id: int, status_update: StatusUpdate):
    return update_transformer_status(transformer_id, status_update.active)
