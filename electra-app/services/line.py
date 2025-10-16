from fastapi import HTTPException
from repositories.lines_repo import get_line_by_id


def get_line(line_id: int):
    row = get_line_by_id(line_id)
    if not row:
        raise HTTPException(status_code=404, detail="Line not found")
    if isinstance(row, tuple):
        (
            id_, grid_id, idtag, name, code, bus_from_idtag, bus_to_idtag, active,
            r, x, b, length,
        ) = row
        return {
            "id": id_,
            "grid_id": grid_id,
            "idtag": idtag,
            "name": name,
            "code": code,
            "bus_from_idtag": bus_from_idtag,
            "bus_to_idtag": bus_to_idtag,
            "active": active,
            "r": r,
            "x": x,
            "b": b,
            "length": length,
        }
    return row
