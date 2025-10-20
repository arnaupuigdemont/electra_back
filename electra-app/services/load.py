from fastapi import HTTPException
from repositories.loads_repo import get_load_by_id, list_loads as repo_list_loads


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


def list_loads():
    rows = repo_list_loads()
    if not rows:
        return []
    normalized = []
    for row in rows:
        if isinstance(row, tuple):
            (
                id_, grid_id, idtag, name, code, bus_idtag, active, p, q, conn, longitude, latitude,
            ) = row
            normalized.append({
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
            })
        else:
            normalized.append(row)
    return normalized
