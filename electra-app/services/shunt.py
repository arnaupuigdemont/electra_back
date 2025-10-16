from fastapi import HTTPException
from repositories.shunts_repo import get_shunt_by_id


def get_shunt(shunt_id: int):
    row = get_shunt_by_id(shunt_id)
    if not row:
        raise HTTPException(status_code=404, detail="Shunt not found")
    if isinstance(row, tuple):
        (
            id_, grid_id, idtag, name, code, bus_idtag, active, b,
        ) = row
        return {
            "id": id_,
            "grid_id": grid_id,
            "idtag": idtag,
            "name": name,
            "code": code,
            "bus_idtag": bus_idtag,
            "active": active,
            "b": b,
        }
    return row
