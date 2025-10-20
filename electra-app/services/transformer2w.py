from fastapi import HTTPException
from repositories.transformers2w_repo import get_transformer2w_by_id, list_transformers2w as repo_list_transformers2w


def get_transformer2w(transformer_id: int):
    row = get_transformer2w_by_id(transformer_id)
    if not row:
        raise HTTPException(status_code=404, detail="Transformer2W not found")
    if isinstance(row, tuple):
        (
            id_, grid_id, idtag, name, code, bus_from_idtag, bus_to_idtag, active,
            r, x, g, b, hv, lv, sn,
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
            "g": g,
            "b": b,
            "hv": hv,
            "lv": lv,
            "sn": sn,
        }
    return row


def list_transformers2w():
    rows = repo_list_transformers2w()
    if not rows:
        return []
    normalized = []
    for row in rows:
        if isinstance(row, tuple):
            (
                id_, grid_id, idtag, name, code, bus_from_idtag, bus_to_idtag, active,
                r, x, g, b, hv, lv, sn,
            ) = row
            normalized.append({
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
                "g": g,
                "b": b,
                "hv": hv,
                "lv": lv,
                "sn": sn,
            })
        else:
            normalized.append(row)
    return normalized
