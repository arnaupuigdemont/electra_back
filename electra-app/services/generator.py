from fastapi import HTTPException
from repositories.generators_repo import get_generator_by_id, list_generators as repo_list_generators, update_generator_status as repo_update_generator_status
import VeraGridEngine as gce
from repositories.grids_repo import get_tmp_file_path
import os
import logging

logger = logging.getLogger(__name__)


def get_generator(generator_id: int):
    row = get_generator_by_id(generator_id)
    if not row:
        raise HTTPException(status_code=404, detail="Generator not found")
    if isinstance(row, tuple):
        (
            id_, grid_id, idtag, name, code, bus_idtag, active, p, vset, qmin, qmax, pf,
            rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
            active_prof, mttf, mttr, capex, opex, build_status, cost, cost_prof, facility,
            technologies, scalable, shift_key, shift_key_prof, longitude, latitude, use_kw,
            conn, rms_model, bus_pos, control_bus, control_bus_prof, p_prof, pmin, pmin_prof,
            pmax, pmax_prof, srap_enabled, srap_enabled_prof, is_controlled, pf_prof, vset_prof,
            snom, qmin_prof, qmax_prof, use_reactive_power_curve, q_curve, r1, x1, r0, x0, r2, x2,
            cost2, cost2_prof, cost0, cost0_prof, startupcost, shutdowncost, mintimeup, mintimedown,
            rampup, rampdown, enabled_dispatch, emissions, fuels,
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
            "control_bus": control_bus,
            "control_bus_prof": control_bus_prof,
            "p_prof": p_prof,
            "pmin": pmin,
            "pmin_prof": pmin_prof,
            "pmax": pmax,
            "pmax_prof": pmax_prof,
            "srap_enabled": srap_enabled,
            "srap_enabled_prof": srap_enabled_prof,
            "is_controlled": is_controlled,
            "pf_prof": pf_prof,
            "vset_prof": vset_prof,
            "snom": snom,
            "qmin_prof": qmin_prof,
            "qmax_prof": qmax_prof,
            "use_reactive_power_curve": use_reactive_power_curve,
            "q_curve": q_curve,
            "r1": r1,
            "x1": x1,
            "r0": r0,
            "x0": x0,
            "r2": r2,
            "x2": x2,
            "cost2": cost2,
            "cost2_prof": cost2_prof,
            "cost0": cost0,
            "cost0_prof": cost0_prof,
            "startupcost": startupcost,
            "shutdowncost": shutdowncost,
            "mintimeup": mintimeup,
            "mintimedown": mintimedown,
            "rampup": rampup,
            "rampdown": rampdown,
            "enabled_dispatch": enabled_dispatch,
            "emissions": emissions,
            "fuels": fuels,
        }
    return row


def list_generators():
    rows = repo_list_generators()
    if not rows:
        return []
    normalized = []
    for row in rows:
        if isinstance(row, tuple):
            (
                id_, grid_id, idtag, name, code, bus_idtag, active, p, vset, qmin, qmax, pf,
                rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
                active_prof, mttf, mttr, capex, opex, build_status, cost, cost_prof, facility,
                technologies, scalable, shift_key, shift_key_prof, longitude, latitude, use_kw,
                conn, rms_model, bus_pos, control_bus, control_bus_prof, p_prof, pmin, pmin_prof,
                pmax, pmax_prof, srap_enabled, srap_enabled_prof, is_controlled, pf_prof, vset_prof,
                snom, qmin_prof, qmax_prof, use_reactive_power_curve, q_curve, r1, x1, r0, x0, r2, x2,
                cost2, cost2_prof, cost0, cost0_prof, startupcost, shutdowncost, mintimeup, mintimedown,
                rampup, rampdown, enabled_dispatch, emissions, fuels,
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
                "vset": vset,
                "qmin": qmin,
                "qmax": qmax,
                "pf": pf,
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
                "control_bus": control_bus,
                "control_bus_prof": control_bus_prof,
                "p_prof": p_prof,
                "pmin": pmin,
                "pmin_prof": pmin_prof,
                "pmax": pmax,
                "pmax_prof": pmax_prof,
                "srap_enabled": srap_enabled,
                "srap_enabled_prof": srap_enabled_prof,
                "is_controlled": is_controlled,
                "pf_prof": pf_prof,
                "vset_prof": vset_prof,
                "snom": snom,
                "qmin_prof": qmin_prof,
                "qmax_prof": qmax_prof,
                "use_reactive_power_curve": use_reactive_power_curve,
                "q_curve": q_curve,
                "r1": r1,
                "x1": x1,
                "r0": r0,
                "x0": x0,
                "r2": r2,
                "x2": x2,
                "cost2": cost2,
                "cost2_prof": cost2_prof,
                "cost0": cost0,
                "cost0_prof": cost0_prof,
                "startupcost": startupcost,
                "shutdowncost": shutdowncost,
                "mintimeup": mintimeup,
                "mintimedown": mintimedown,
                "rampup": rampup,
                "rampdown": rampdown,
                "enabled_dispatch": enabled_dispatch,
                "emissions": emissions,
                "fuels": fuels,
            })
        else:
            normalized.append(row)
    return normalized


def update_generator_status(generator_id: int, active: bool):
    """Update the active status of a generator."""
    # Get generator details to know grid_id and idtag
    gen_row = get_generator_by_id(generator_id)
    if not gen_row:
        raise HTTPException(status_code=404, detail="Generator not found")
    
    # Extract grid_id and idtag from the row
    if isinstance(gen_row, tuple):
        grid_id = gen_row[1]  # grid_id is the second column
        gen_idtag = gen_row[2]  # idtag is the third column
    else:
        grid_id = gen_row.get('grid_id')
        gen_idtag = gen_row.get('idtag')
    
    # Update the generator status in database
    result = repo_update_generator_status(generator_id, active)
    if not result:
        raise HTTPException(status_code=404, detail="Generator not found")
    
    # Update VeraGrid circuit file
    try:
        tmp_path = get_tmp_file_path(grid_id)
        if tmp_path and os.path.exists(tmp_path):
            # Load the circuit
            circuit = gce.open_file(tmp_path)
            
            # Find the generator by idtag
            for gen in circuit.generators:
                if gen.idtag == gen_idtag:
                    gen.active = active
                    break
            
            # Save the updated circuit
            gce.save_file(circuit, tmp_path)
            logger.info(f"Updated generator {gen_idtag} status to {active} in VeraGrid circuit")
    except Exception as e:
        logger.error(f"Error updating VeraGrid circuit: {e}")
    
    return {"message": "Generator status updated", "generator_id": generator_id, "active": active}
