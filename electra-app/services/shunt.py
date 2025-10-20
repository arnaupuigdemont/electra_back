from fastapi import HTTPException
from repositories.shunts_repo import get_shunt_by_id, list_shunts as repo_list_shunts


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


def list_shunts():
    rows = repo_list_shunts()
    if not rows:
        return []
    normalized = []
    for row in rows:
        if isinstance(row, tuple):
            (
                id_, grid_id, idtag, name, code, bus_idtag, active, b,
            ) = row
            normalized.append({
                "id": id_,
                "grid_id": grid_id,
                "idtag": idtag,
                "name": name,
                "code": code,
                "bus_idtag": bus_idtag,
                "active": active,
                "b": b,
            })
        else:
            normalized.append(row)
    return normalized
