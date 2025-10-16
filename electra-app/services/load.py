from fastapi import HTTPException
from repositories.loads_repo import get_load_by_id


def get_load(load_id: int):
    row = get_load_by_id(load_id)
    if not row:
        raise HTTPException(status_code=404, detail="Load not found")
    if isinstance(row, tuple):
        (
            id_, grid_id, idtag, name, code, bus_idtag, active, p, q, conn, longitude, latitude,
        ) = row
        return {
            "id": id_,
            "grid_id": grid_id,
            "idtag": idtag,
            "name": name,
            "code": code,
            "bus_idtag": bus_idtag,
            "active": active,
            "p": p,
            "q": q,
            "conn": conn,
            "longitude": longitude,
            "latitude": latitude,
        }
    return row
