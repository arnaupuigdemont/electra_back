from fastapi import HTTPException
from repositories.buses_repo import get_bus_by_id as repo_get_bus_by_id


def get_bus(bus_id: int):
    row = repo_get_bus_by_id(bus_id)
    if not row:
        raise HTTPException(status_code=404, detail="Bus not found")

    # Convert tuple to dict if using default cursor
    if isinstance(row, tuple):
        (
            id_, grid_id, idtag, name, code, vnom, vm0, va0, x, y,
            longitude, latitude, is_slack,
        ) = row
        return {
            "id": id_,
            "grid_id": grid_id,
            "idtag": idtag,
            "name": name,
            "code": code,
            "vnom": vnom,
            "vm0": vm0,
            "va0": va0,
            "x": x,
            "y": y,
            "longitude": longitude,
            "latitude": latitude,
            "is_slack": is_slack,
        }
    return row
