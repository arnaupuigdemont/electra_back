from fastapi import HTTPException
from repositories.generators_repo import get_generator_by_id


def get_generator(generator_id: int):
    row = get_generator_by_id(generator_id)
    if not row:
        raise HTTPException(status_code=404, detail="Generator not found")
    if isinstance(row, tuple):
        (
            id_, grid_id, idtag, name, code, bus_idtag, active, p, vset, qmin, qmax, pf,
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
            "vset": vset,
            "qmin": qmin,
            "qmax": qmax,
            "pf": pf,
        }
    return row
