from typing import Annotated
from fastapi import APIRouter, FastAPI, File, UploadFile

router = APIRouter()

@router.post("/files/upload")
async def create_file(file: Annotated[bytes, File()]):
    return {"file_size": len(file)}

