from fastapi import HTTPException
from repositories.transformers2w_repo import get_transformer2w_by_id, list_transformers2w as repo_list_transformers2w, update_transformer_status as repo_update_transformer_status
import VeraGridEngine as gce
from repositories.grids_repo import get_tmp_file_path
import os
import logging

logger = logging.getLogger(__name__)


def get_transformer2w(transformer_id: int):
    row = get_transformer2w_by_id(transformer_id)
    if not row:
        raise HTTPException(status_code=404, detail="Transformer2W not found")
    if isinstance(row, tuple):
        (
            id_, grid_id, idtag, name, code, bus_from_idtag, bus_to_idtag, active,
            r, x, g, b, hv, lv, sn,
            rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
            active_prof, reducible, rate, rate_prof, contingency_factor, contingency_factor_prof,
            protection_rating_factor, protection_rating_factor_prof, monitor_loading, mttf, mttr,
            cost, cost_prof, build_status, capex, opex, tx_group, color, rms_model,
            bus_from_pos, bus_to_pos, r0, x0, g0, b0, r2, x2, g2, b2, tolerance,
            tap_changer, tap_module, tap_module_prof, tap_module_max, tap_module_min,
            tap_module_control_mode, tap_module_control_mode_prof, vset, vset_prof,
            qset, qset_prof, regulation_bus, tap_phase, tap_phase_prof, tap_phase_max,
            tap_phase_min, tap_phase_control_mode, tap_phase_control_mode_prof, pset,
            pset_prof, temp_base, temp_oper, temp_oper_prof, alpha, pcu, pfe, i0, vsc,
            conn, conn_f, conn_t, vector_group_number, template,
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
            "rdfid": rdfid,
            "action": action,
            "comment": comment,
            "modelling_authority": modelling_authority,
            "commissioned_date": commissioned_date,
            "decommissioned_date": decommissioned_date,
            "active_prof": active_prof,
            "reducible": reducible,
            "rate": rate,
            "rate_prof": rate_prof,
            "contingency_factor": contingency_factor,
            "contingency_factor_prof": contingency_factor_prof,
            "protection_rating_factor": protection_rating_factor,
            "protection_rating_factor_prof": protection_rating_factor_prof,
            "monitor_loading": monitor_loading,
            "mttf": mttf,
            "mttr": mttr,
            "cost": cost,
            "cost_prof": cost_prof,
            "build_status": build_status,
            "capex": capex,
            "opex": opex,
            "tx_group": tx_group,
            "color": color,
            "rms_model": rms_model,
            "bus_from_pos": bus_from_pos,
            "bus_to_pos": bus_to_pos,
            "r0": r0,
            "x0": x0,
            "g0": g0,
            "b0": b0,
            "r2": r2,
            "x2": x2,
            "g2": g2,
            "b2": b2,
            "tolerance": tolerance,
            "tap_changer": tap_changer,
            "tap_module": tap_module,
            "tap_module_prof": tap_module_prof,
            "tap_module_max": tap_module_max,
            "tap_module_min": tap_module_min,
            "tap_module_control_mode": tap_module_control_mode,
            "tap_module_control_mode_prof": tap_module_control_mode_prof,
            "vset": vset,
            "vset_prof": vset_prof,
            "qset": qset,
            "qset_prof": qset_prof,
            "regulation_bus": regulation_bus,
            "tap_phase": tap_phase,
            "tap_phase_prof": tap_phase_prof,
            "tap_phase_max": tap_phase_max,
            "tap_phase_min": tap_phase_min,
            "tap_phase_control_mode": tap_phase_control_mode,
            "tap_phase_control_mode_prof": tap_phase_control_mode_prof,
            "pset": pset,
            "pset_prof": pset_prof,
            "temp_base": temp_base,
            "temp_oper": temp_oper,
            "temp_oper_prof": temp_oper_prof,
            "alpha": alpha,
            "pcu": pcu,
            "pfe": pfe,
            "i0": i0,
            "vsc": vsc,
            "conn": conn,
            "conn_f": conn_f,
            "conn_t": conn_t,
            "vector_group_number": vector_group_number,
            "template": template,
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
                rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
                active_prof, reducible, rate, rate_prof, contingency_factor, contingency_factor_prof,
                protection_rating_factor, protection_rating_factor_prof, monitor_loading, mttf, mttr,
                cost, cost_prof, build_status, capex, opex, tx_group, color, rms_model,
                bus_from_pos, bus_to_pos, r0, x0, g0, b0, r2, x2, g2, b2, tolerance,
                tap_changer, tap_module, tap_module_prof, tap_module_max, tap_module_min,
                tap_module_control_mode, tap_module_control_mode_prof, vset, vset_prof,
                qset, qset_prof, regulation_bus, tap_phase, tap_phase_prof, tap_phase_max,
                tap_phase_min, tap_phase_control_mode, tap_phase_control_mode_prof, pset,
                pset_prof, temp_base, temp_oper, temp_oper_prof, alpha, pcu, pfe, i0, vsc,
                conn, conn_f, conn_t, vector_group_number, template,
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
                "rdfid": rdfid,
                "action": action,
                "comment": comment,
                "modelling_authority": modelling_authority,
                "commissioned_date": commissioned_date,
                "decommissioned_date": decommissioned_date,
                "active_prof": active_prof,
                "reducible": reducible,
                "rate": rate,
                "rate_prof": rate_prof,
                "contingency_factor": contingency_factor,
                "contingency_factor_prof": contingency_factor_prof,
                "protection_rating_factor": protection_rating_factor,
                "protection_rating_factor_prof": protection_rating_factor_prof,
                "monitor_loading": monitor_loading,
                "mttf": mttf,
                "mttr": mttr,
                "cost": cost,
                "cost_prof": cost_prof,
                "build_status": build_status,
                "capex": capex,
                "opex": opex,
                "tx_group": tx_group,
                "color": color,
                "rms_model": rms_model,
                "bus_from_pos": bus_from_pos,
                "bus_to_pos": bus_to_pos,
                "r0": r0,
                "x0": x0,
                "g0": g0,
                "b0": b0,
                "r2": r2,
                "x2": x2,
                "g2": g2,
                "b2": b2,
                "tolerance": tolerance,
                "tap_changer": tap_changer,
                "tap_module": tap_module,
                "tap_module_prof": tap_module_prof,
                "tap_module_max": tap_module_max,
                "tap_module_min": tap_module_min,
                "tap_module_control_mode": tap_module_control_mode,
                "tap_module_control_mode_prof": tap_module_control_mode_prof,
                "vset": vset,
                "vset_prof": vset_prof,
                "qset": qset,
                "qset_prof": qset_prof,
                "regulation_bus": regulation_bus,
                "tap_phase": tap_phase,
                "tap_phase_prof": tap_phase_prof,
                "tap_phase_max": tap_phase_max,
                "tap_phase_min": tap_phase_min,
                "tap_phase_control_mode": tap_phase_control_mode,
                "tap_phase_control_mode_prof": tap_phase_control_mode_prof,
                "pset": pset,
                "pset_prof": pset_prof,
                "temp_base": temp_base,
                "temp_oper": temp_oper,
                "temp_oper_prof": temp_oper_prof,
                "alpha": alpha,
                "pcu": pcu,
                "pfe": pfe,
                "i0": i0,
                "vsc": vsc,
                "conn": conn,
                "conn_f": conn_f,
                "conn_t": conn_t,
                "vector_group_number": vector_group_number,
                "template": template,
            })
        else:
            normalized.append(row)
    return normalized


def update_transformer_status(transformer_id: int, active: bool):
    """Update the active status of a transformer."""
    # Get transformer details to know grid_id and idtag
    transformer_row = get_transformer2w_by_id(transformer_id)
    if not transformer_row:
        raise HTTPException(status_code=404, detail="Transformer not found")
    
    # Extract grid_id and idtag from the row
    if isinstance(transformer_row, tuple):
        grid_id = transformer_row[1]  # grid_id is the second column
        transformer_idtag = transformer_row[2]  # idtag is the third column
    else:
        grid_id = transformer_row.get('grid_id')
        transformer_idtag = transformer_row.get('idtag')
    
    # Update the transformer status in database
    result = repo_update_transformer_status(transformer_id, active)
    if not result:
        raise HTTPException(status_code=404, detail="Transformer not found")
    
    # Update VeraGrid circuit file
    try:
        tmp_path = get_tmp_file_path(grid_id)
        if tmp_path and os.path.exists(tmp_path):
            # Load the circuit
            circuit = gce.open_file(tmp_path)
            
            # Find the transformer by idtag
            for transformer in circuit.transformers2w:
                if transformer.idtag == transformer_idtag:
                    transformer.active = active
                    break
            
            # Save the updated circuit
            gce.save_file(circuit, tmp_path)
            logger.info(f"Updated transformer {transformer_idtag} status to {active} in VeraGrid circuit")
    except Exception as e:
        logger.error(f"Error updating VeraGrid circuit: {e}")
    
    return {"message": "Transformer status updated", "transformer_id": transformer_id, "active": active}
