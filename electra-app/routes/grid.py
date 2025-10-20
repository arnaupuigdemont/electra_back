from typing import Annotated
from fastapi import APIRouter, FastAPI, File, UploadFile
from services import grid

router = APIRouter()

@router.post("/files/upload")
async def upload_file(file: UploadFile = File(...)):
    return await grid.upload_file(file)

@router.get("/ids")
def get_grid_ids():
    return grid.list_grid_ids()

@router.delete("/{grid_id}")
def delete_grid(grid_id: int):
    grid.delete_grid(grid_id)
    return {"deleted": True, "grid_id": grid_id}



