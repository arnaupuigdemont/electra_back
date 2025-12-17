from fastapi import HTTPException
from repositories.loads_repo import get_load_by_id, list_loads as repo_list_loads, update_load_status as repo_update_load_status
import VeraGridEngine as gce
from repositories.grids_repo import get_tmp_file_path
import os
import logging

logger = logging.getLogger(__name__)


def get_load(load_id: int):
    row = get_load_by_id(load_id)
    if not row:
        raise HTTPException(status_code=404, detail="Load not found")
    if isinstance(row, tuple):
        (
            id_, grid_id, idtag, name, code, bus_idtag, active, p, q, conn, longitude, latitude,
            rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
            active_prof, mttf, mttr, capex, opex, build_status, cost, cost_prof, facility,
            technologies, scalable, shift_key, shift_key_prof, use_kw, rms_model, bus_pos,
            p_prof, pa, pa_prof, pb, pb_prof, pc, pc_prof, q_prof, qa, qa_prof, qb, qb_prof,
            qc, qc_prof, ir, ir_prof, ir1, ir1_prof, ir2, ir2_prof, ir3, ir3_prof, ii, ii_prof,
            ii1, ii1_prof, ii2, ii2_prof, ii3, ii3_prof, g, g_prof, g1, g1_prof, g2, g2_prof,
            g3, g3_prof, b, b_prof, b1, b1_prof, b2, b2_prof, b3, b3_prof, n_customers, n_customers_prof,
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
            "use_kw": use_kw,
            "rms_model": rms_model,
            "bus_pos": bus_pos,
            "p_prof": p_prof,
            "pa": pa,
            "pa_prof": pa_prof,
            "pb": pb,
            "pb_prof": pb_prof,
            "pc": pc,
            "pc_prof": pc_prof,
            "q_prof": q_prof,
            "qa": qa,
            "qa_prof": qa_prof,
            "qb": qb,
            "qb_prof": qb_prof,
            "qc": qc,
            "qc_prof": qc_prof,
            "ir": ir,
            "ir_prof": ir_prof,
            "ir1": ir1,
            "ir1_prof": ir1_prof,
            "ir2": ir2,
            "ir2_prof": ir2_prof,
            "ir3": ir3,
            "ir3_prof": ir3_prof,
            "ii": ii,
            "ii_prof": ii_prof,
            "ii1": ii1,
            "ii1_prof": ii1_prof,
            "ii2": ii2,
            "ii2_prof": ii2_prof,
            "ii3": ii3,
            "ii3_prof": ii3_prof,
            "g": g,
            "g_prof": g_prof,
            "g1": g1,
            "g1_prof": g1_prof,
            "g2": g2,
            "g2_prof": g2_prof,
            "g3": g3,
            "g3_prof": g3_prof,
            "b": b,
            "b_prof": b_prof,
            "b1": b1,
            "b1_prof": b1_prof,
            "b2": b2,
            "b2_prof": b2_prof,
            "b3": b3,
            "b3_prof": b3_prof,
            "n_customers": n_customers,
            "n_customers_prof": n_customers_prof,
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
                rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
                active_prof, mttf, mttr, capex, opex, build_status, cost, cost_prof, facility,
                technologies, scalable, shift_key, shift_key_prof, use_kw, rms_model, bus_pos,
                p_prof, pa, pa_prof, pb, pb_prof, pc, pc_prof, q_prof, qa, qa_prof, qb, qb_prof,
                qc, qc_prof, ir, ir_prof, ir1, ir1_prof, ir2, ir2_prof, ir3, ir3_prof, ii, ii_prof,
                ii1, ii1_prof, ii2, ii2_prof, ii3, ii3_prof, g, g_prof, g1, g1_prof, g2, g2_prof,
                g3, g3_prof, b, b_prof, b1, b1_prof, b2, b2_prof, b3, b3_prof, n_customers, n_customers_prof,
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
                "use_kw": use_kw,
                "rms_model": rms_model,
                "bus_pos": bus_pos,
                "p_prof": p_prof,
                "pa": pa,
                "pa_prof": pa_prof,
                "pb": pb,
                "pb_prof": pb_prof,
                "pc": pc,
                "pc_prof": pc_prof,
                "q_prof": q_prof,
                "qa": qa,
                "qa_prof": qa_prof,
                "qb": qb,
                "qb_prof": qb_prof,
                "qc": qc,
                "qc_prof": qc_prof,
                "ir": ir,
                "ir_prof": ir_prof,
                "ir1": ir1,
                "ir1_prof": ir1_prof,
                "ir2": ir2,
                "ir2_prof": ir2_prof,
                "ir3": ir3,
                "ir3_prof": ir3_prof,
                "ii": ii,
                "ii_prof": ii_prof,
                "ii1": ii1,
                "ii1_prof": ii1_prof,
                "ii2": ii2,
                "ii2_prof": ii2_prof,
                "ii3": ii3,
                "ii3_prof": ii3_prof,
                "g": g,
                "g_prof": g_prof,
                "g1": g1,
                "g1_prof": g1_prof,
                "g2": g2,
                "g2_prof": g2_prof,
                "g3": g3,
                "g3_prof": g3_prof,
                "b": b,
                "b_prof": b_prof,
                "b1": b1,
                "b1_prof": b1_prof,
                "b2": b2,
                "b2_prof": b2_prof,
                "b3": b3,
                "b3_prof": b3_prof,
                "n_customers": n_customers,
                "n_customers_prof": n_customers_prof,
            })
        else:
            normalized.append(row)
    return normalized


def update_load_status(load_id: int, active: bool):
    """Update the active status of a load."""
    # Get load details to know grid_id and idtag
    load_row = get_load_by_id(load_id)
    if not load_row:
        raise HTTPException(status_code=404, detail="Load not found")
    
    # Extract grid_id and idtag from the row
    if isinstance(load_row, tuple):
        grid_id = load_row[1]  # grid_id is the second column
        load_idtag = load_row[2]  # idtag is the third column
    else:
        grid_id = load_row.get('grid_id')
        load_idtag = load_row.get('idtag')
    
    # Update the load status in database
    result = repo_update_load_status(load_id, active)
    if not result:
        raise HTTPException(status_code=404, detail="Load not found")
    
    # Update VeraGrid circuit file
    try:
        tmp_path = get_tmp_file_path(grid_id)
        if tmp_path and os.path.exists(tmp_path):
            # Load the circuit
            circuit = gce.open_file(tmp_path)
            
            # Find the load by idtag
            for load in circuit.loads:
                if load.idtag == load_idtag:
                    load.active = active
                    break
            
            # Save the updated circuit
            gce.save_file(circuit, tmp_path)
            logger.info(f"Updated load {load_idtag} status to {active} in VeraGrid circuit")
    except Exception as e:
        logger.error(f"Error updating VeraGrid circuit: {e}")
    
    return {"message": "Load status updated", "load_id": load_id, "active": active}
