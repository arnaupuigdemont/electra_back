from fastapi import APIRouter, Response, status
from services.health import db_health_check

router = APIRouter()

@router.get("/db")
def health_db(response: Response):
    result = db_health_check()
    if result.get("status") == "ok":
        response.status_code = status.HTTP_200_OK
    else:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return result
