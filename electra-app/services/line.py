from fastapi import HTTPException
from repositories.lines_repo import get_line_by_id, list_lines as repo_list_lines, update_line_status as repo_update_line_status
import VeraGridEngine as gce
from repositories.grids_repo import get_tmp_file_path
import os
import logging

logger = logging.getLogger(__name__)


def get_line(line_id: int):
    row = get_line_by_id(line_id)
    if not row:
        raise HTTPException(status_code=404, detail="Line not found")
    if isinstance(row, tuple):
        (
            id_, grid_id, idtag, name, code, bus_from_idtag, bus_to_idtag, active,
            r, x, b, length,
            rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
            active_prof, reducible, rate, rate_prof, contingency_factor, contingency_factor_prof,
            protection_rating_factor, protection_rating_factor_prof, monitor_loading, mttf, mttr,
            cost, cost_prof, build_status, capex, opex, line_group, color, rms_model,
            bus_from_pos, bus_to_pos, r0, x0, b0, r2, x2, b2, ys, ysh, tolerance, circuit_idx,
            temp_base, temp_oper, temp_oper_prof, alpha, r_fault, x_fault, fault_pos, template,
            locations, possible_tower_types, possible_underground_line_types, possible_sequence_line_types,
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
            "line_group": line_group,
            "color": color,
            "rms_model": rms_model,
            "bus_from_pos": bus_from_pos,
            "bus_to_pos": bus_to_pos,
            "r0": r0,
            "x0": x0,
            "b0": b0,
            "r2": r2,
            "x2": x2,
            "b2": b2,
            "ys": ys,
            "ysh": ysh,
            "tolerance": tolerance,
            "circuit_idx": circuit_idx,
            "temp_base": temp_base,
            "temp_oper": temp_oper,
            "temp_oper_prof": temp_oper_prof,
            "alpha": alpha,
            "r_fault": r_fault,
            "x_fault": x_fault,
            "fault_pos": fault_pos,
            "template": template,
            "locations": locations,
            "possible_tower_types": possible_tower_types,
            "possible_underground_line_types": possible_underground_line_types,
            "possible_sequence_line_types": possible_sequence_line_types,
        }
    return row


def list_lines():
    rows = repo_list_lines()
    if not rows:
        return []
    normalized = []
    for row in rows:
        if isinstance(row, tuple):
            (
                id_, grid_id, idtag, name, code, bus_from_idtag, bus_to_idtag, active,
                r, x, b, length,
                rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
                active_prof, reducible, rate, rate_prof, contingency_factor, contingency_factor_prof,
                protection_rating_factor, protection_rating_factor_prof, monitor_loading, mttf, mttr,
                cost, cost_prof, build_status, capex, opex, line_group, color, rms_model,
                bus_from_pos, bus_to_pos, r0, x0, b0, r2, x2, b2, ys, ysh, tolerance, circuit_idx,
                temp_base, temp_oper, temp_oper_prof, alpha, r_fault, x_fault, fault_pos, template,
                locations, possible_tower_types, possible_underground_line_types, possible_sequence_line_types,
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
                "b": b,
                "length": length,
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
                "line_group": line_group,
                "color": color,
                "rms_model": rms_model,
                "bus_from_pos": bus_from_pos,
                "bus_to_pos": bus_to_pos,
                "r0": r0,
                "x0": x0,
                "b0": b0,
                "r2": r2,
                "x2": x2,
                "b2": b2,
                "ys": ys,
                "ysh": ysh,
                "tolerance": tolerance,
                "circuit_idx": circuit_idx,
                "temp_base": temp_base,
                "temp_oper": temp_oper,
                "temp_oper_prof": temp_oper_prof,
                "alpha": alpha,
                "r_fault": r_fault,
                "x_fault": x_fault,
                "fault_pos": fault_pos,
                "template": template,
                "locations": locations,
                "possible_tower_types": possible_tower_types,
                "possible_underground_line_types": possible_underground_line_types,
                "possible_sequence_line_types": possible_sequence_line_types,
            })
        else:
            normalized.append(row)
    return normalized


def update_line_status(line_id: int, active: bool):
    """Update the active status of a line."""
    # Get line details to know grid_id and idtag
    line_row = get_line_by_id(line_id)
    if not line_row:
        raise HTTPException(status_code=404, detail="Line not found")
    
    # Extract grid_id and idtag from the row
    if isinstance(line_row, tuple):
        grid_id = line_row[1]  # grid_id is the second column
        line_idtag = line_row[2]  # idtag is the third column
    else:
        grid_id = line_row.get('grid_id')
        line_idtag = line_row.get('idtag')
    
    # Update the line status in database
    result = repo_update_line_status(line_id, active)
    if not result:
        raise HTTPException(status_code=404, detail="Line not found")
    
    # Update VeraGrid circuit file
    try:
        tmp_path = get_tmp_file_path(grid_id)
        if tmp_path and os.path.exists(tmp_path):
            # Load the circuit
            circuit = gce.open_file(tmp_path)
            
            # Find the line by idtag
            for line in circuit.lines:
                if line.idtag == line_idtag:
                    line.active = active
                    break
            
            # Save the updated circuit
            gce.save_file(circuit, tmp_path)
            logger.info(f"Updated line {line_idtag} status to {active} in VeraGrid circuit")
    except Exception as e:
        logger.error(f"Error updating VeraGrid circuit: {e}")
    
    return {"message": "Line status updated", "line_id": line_id, "active": active}
