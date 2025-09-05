from pydantic import BaseModel

# Modelos específicos para GridCal
class GridCalFileInfo(BaseModel):
    filename: str
    file_size: int
    upload_timestamp: str
    file_path: str
    status: str = "uploaded"

class FileUploadResponse(BaseModel):
    message: str
    file_info: GridCalFileInfo
    success: bool
