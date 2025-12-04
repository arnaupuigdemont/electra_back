from fastapi import HTTPException
from repositories.shunts_repo import get_shunt_by_id, list_shunts as repo_list_shunts


def get_shunt(shunt_id: int):
    row = get_shunt_by_id(shunt_id)
    if not row:
        raise HTTPException(status_code=404, detail="Shunt not found")
    if isinstance(row, tuple):
        (
            id_, grid_id, idtag, name, code, bus_idtag, active, b,
            rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
            active_prof, mttf, mttr, capex, opex, build_status, cost, cost_prof, facility,
            technologies, scalable, shift_key, shift_key_prof, longitude, latitude, use_kw,
            conn, rms_model, bus_pos, g, g_prof, g0, g0_prof, ga, ga_prof, gb, gb_prof,
            gc, gc_prof, b_prof, b0, b0_prof, ba, ba_prof, bb, bb_prof, bc, bc_prof, ysh,
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
            "rdfid": rdfid,
            "action": action,
            "comment": comment,
            "modelling_authority": modelling_authority,
            "commissioned_date": commissioned_date,
            "decommissioned_date": decommissioned_date,
            "active_prof": active_prof,
            "mttf": mttf,
            "mttr": mttr,
            "capex": capex,
            "opex": opex,
            "build_status": build_status,
            "cost": cost,
            "cost_prof": cost_prof,
            "facility": facility,
            "technologies": technologies,
            "scalable": scalable,
            "shift_key": shift_key,
            "shift_key_prof": shift_key_prof,
            "longitude": longitude,
            "latitude": latitude,
            "use_kw": use_kw,
            "conn": conn,
            "rms_model": rms_model,
            "bus_pos": bus_pos,
            "g": g,
            "g_prof": g_prof,
            "g0": g0,
            "g0_prof": g0_prof,
            "ga": ga,
            "ga_prof": ga_prof,
            "gb": gb,
            "gb_prof": gb_prof,
            "gc": gc,
            "gc_prof": gc_prof,
            "b_prof": b_prof,
            "b0": b0,
            "b0_prof": b0_prof,
            "ba": ba,
            "ba_prof": ba_prof,
            "bb": bb,
            "bb_prof": bb_prof,
            "bc": bc,
            "bc_prof": bc_prof,
            "ysh": ysh,
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
                rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
                active_prof, mttf, mttr, capex, opex, build_status, cost, cost_prof, facility,
                technologies, scalable, shift_key, shift_key_prof, longitude, latitude, use_kw,
                conn, rms_model, bus_pos, g, g_prof, g0, g0_prof, ga, ga_prof, gb, gb_prof,
                gc, gc_prof, b_prof, b0, b0_prof, ba, ba_prof, bb, bb_prof, bc, bc_prof, ysh,
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
                "rdfid": rdfid,
                "action": action,
                "comment": comment,
                "modelling_authority": modelling_authority,
                "commissioned_date": commissioned_date,
                "decommissioned_date": decommissioned_date,
                "active_prof": active_prof,
                "mttf": mttf,
                "mttr": mttr,
                "capex": capex,
                "opex": opex,
                "build_status": build_status,
                "cost": cost,
                "cost_prof": cost_prof,
                "facility": facility,
                "technologies": technologies,
                "scalable": scalable,
                "shift_key": shift_key,
                "shift_key_prof": shift_key_prof,
                "longitude": longitude,
                "latitude": latitude,
                "use_kw": use_kw,
                "conn": conn,
                "rms_model": rms_model,
                "bus_pos": bus_pos,
                "g": g,
                "g_prof": g_prof,
                "g0": g0,
                "g0_prof": g0_prof,
                "ga": ga,
                "ga_prof": ga_prof,
                "gb": gb,
                "gb_prof": gb_prof,
                "gc": gc,
                "gc_prof": gc_prof,
                "b_prof": b_prof,
                "b0": b0,
                "b0_prof": b0_prof,
                "ba": ba,
                "ba_prof": ba_prof,
                "bb": bb,
                "bb_prof": bb_prof,
                "bc": bc,
                "bc_prof": bc_prof,
                "ysh": ysh,
            })
        else:
            normalized.append(row)
    return normalized
