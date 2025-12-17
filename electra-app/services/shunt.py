from fastapi import HTTPException
from repositories.shunts_repo import get_shunt_by_id, list_shunts as repo_list_shunts, update_shunt_status as repo_update_shunt_status
import VeraGridEngine as gce
from repositories.grids_repo import get_tmp_file_path
import os
import logging

logger = logging.getLogger(__name__)


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


def update_shunt_status(shunt_id: int, active: bool):
    """Update the active status of a shunt."""
    # Get shunt details to know grid_id and idtag
    shunt_row = get_shunt_by_id(shunt_id)
    if not shunt_row:
        raise HTTPException(status_code=404, detail="Shunt not found")
    
    # Extract grid_id and idtag from the row
    if isinstance(shunt_row, tuple):
        grid_id = shunt_row[1]  # grid_id is the second column
        shunt_idtag = shunt_row[2]  # idtag is the third column
    else:
        grid_id = shunt_row.get('grid_id')
        shunt_idtag = shunt_row.get('idtag')
    
    # Update the shunt status in database
    result = repo_update_shunt_status(shunt_id, active)
    if not result:
        raise HTTPException(status_code=404, detail="Shunt not found")
    
    # Update VeraGrid circuit file
    try:
        tmp_path = get_tmp_file_path(grid_id)
        if tmp_path and os.path.exists(tmp_path):
            # Load the circuit
            circuit = gce.open_file(tmp_path)
            
            # Find the shunt by idtag
            for shunt in circuit.shunts:
                if shunt.idtag == shunt_idtag:
                    shunt.active = active
                    break
            
            # Save the updated circuit
            gce.save_file(circuit, tmp_path)
            logger.info(f"Updated shunt {shunt_idtag} status to {active} in VeraGrid circuit")
    except Exception as e:
        logger.error(f"Error updating VeraGrid circuit: {e}")
    
    return {"message": "Shunt status updated", "shunt_id": shunt_id, "active": active}


def update_shunt_status(shunt_id: int, active: bool):
    """Update the active status of a shunt."""
    # Get shunt details to know grid_id and idtag
    shunt_row = get_shunt_by_id(shunt_id)
    if not shunt_row:
        raise HTTPException(status_code=404, detail="Shunt not found")
    
    # Extract grid_id and idtag from the row
    if isinstance(shunt_row, tuple):
        grid_id = shunt_row[1]  # grid_id is the second column
        shunt_idtag = shunt_row[2]  # idtag is the third column
    else:
        grid_id = shunt_row.get('grid_id')
        shunt_idtag = shunt_row.get('idtag')
    
    # Update the shunt status in database
    result = repo_update_shunt_status(shunt_id, active)
    if not result:
        raise HTTPException(status_code=404, detail="Shunt not found")
    
    # Update VeraGrid circuit file
    try:
        tmp_path = get_tmp_file_path(grid_id)
        if tmp_path and os.path.exists(tmp_path):
            # Load the circuit
            circuit = gce.open_file(tmp_path)
            
            # Find the shunt by idtag
            for shunt in circuit.shunts:
                if shunt.idtag == shunt_idtag:
                    shunt.active = active
                    break
            
            # Save the updated circuit
            gce.save_file(circuit, tmp_path)
            logger.info(f"Updated shunt {shunt_idtag} status to {active} in VeraGrid circuit")
    except Exception as e:
        logger.error(f"Error updating VeraGrid circuit: {e}")
    
    return {"message": "Shunt status updated", "shunt_id": shunt_id, "active": active}
