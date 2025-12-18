from fastapi import HTTPException
from repositories.buses_repo import (
    get_bus_by_id as repo_get_bus_by_id,
    list_buses as repo_list_buses,
    update_bus_status as repo_update_bus_status,
    update_elements_by_bus_idtag as repo_update_elements_by_bus_idtag,
)
import VeraGridEngine as gce
from repositories.grids_repo import get_tmp_file_path
import os
import logging

logger = logging.getLogger(__name__)

def list_buses():
    rows = repo_list_buses()
    if not rows:
        return []
    normalized = []
    for row in rows:
        if isinstance(row, tuple):
            (
                id_, grid_id, idtag, name, code, vnom, vm0, va0, vmin, vmax, vm_cost,
                angle_min, angle_max, angle_cost, r_fault, x_fault,
                x, y, longitude, latitude, is_slack, active, is_dc, graphic_type, h, w,
                country, area, zone, substation, voltage_level, bus_bar, ph_a, ph_b, ph_c, ph_n,
                is_grounded, active_prof, vmin_prof, vmax_prof,
            ) = row
            normalized.append({
                "id": id_,
                "grid_id": grid_id,
                "idtag": idtag,
                "name": name,
                "code": code,
                "vnom": vnom,
                "vm0": vm0,
                "va0": va0,
                "vmin": vmin,
                "vmax": vmax,
                "vm_cost": vm_cost,
                "angle_min": angle_min,
                "angle_max": angle_max,
                "angle_cost": angle_cost,
                "r_fault": r_fault,
                "x_fault": x_fault,
                "x": x,
                "y": y,
                "longitude": longitude,
                "latitude": latitude,
                "is_slack": is_slack,
                "active": active,
                "is_dc": is_dc,
                "graphic_type": graphic_type,
                "h": h,
                "w": w,
                "country": country,
                "area": area,
                "zone": zone,
                "substation": substation,
                "voltage_level": voltage_level,
                "bus_bar": bus_bar,
                "ph_a": ph_a,
                "ph_b": ph_b,
                "ph_c": ph_c,
                "ph_n": ph_n,
                "is_grounded": is_grounded,
                "active_prof": active_prof,
                "vmin_prof": vmin_prof,
                "vmax_prof": vmax_prof,
            })
        else:
            # Ensure we return a fresh dict per row to avoid any cursor row reuse issues
            try:
                normalized.append(dict(row))
            except Exception:
                normalized.append(row)
    return normalized

def get_bus(bus_id: int):
    row = repo_get_bus_by_id(bus_id)
    if not row:
        raise HTTPException(status_code=404, detail="Bus not found")
    
    if isinstance(row, tuple):
        (
            id_, grid_id, idtag, name, code, vnom, vm0, va0, vmin, vmax, vm_cost,
            angle_min, angle_max, angle_cost, r_fault, x_fault,
            x, y, longitude, latitude, is_slack, active, is_dc, graphic_type, h, w,
            country, area, zone, substation, voltage_level, bus_bar, ph_a, ph_b, ph_c, ph_n,
            is_grounded, active_prof, vmin_prof, vmax_prof,
        ) = row
        return {
            "id": id_,
            "grid_id": grid_id,
            "idtag": idtag,
            "name": name,
            "code": code,
            "vnom": vnom,
            "vm0": vm0,
            "va0": va0,
            "vmin": vmin,
            "vmax": vmax,
            "vm_cost": vm_cost,
            "angle_min": angle_min,
            "angle_max": angle_max,
            "angle_cost": angle_cost,
            "r_fault": r_fault,
            "x_fault": x_fault,
            "x": x,
            "y": y,
            "longitude": longitude,
            "latitude": latitude,
            "is_slack": is_slack,
            "active": active,
            "is_dc": is_dc,
            "graphic_type": graphic_type,
            "h": h,
            "w": w,
            "country": country,
            "area": area,
            "zone": zone,
            "substation": substation,
            "voltage_level": voltage_level,
            "bus_bar": bus_bar,
            "ph_a": ph_a,
            "ph_b": ph_b,
            "ph_c": ph_c,
            "ph_n": ph_n,
            "is_grounded": is_grounded,
            "active_prof": active_prof,
            "vmin_prof": vmin_prof,
            "vmax_prof": vmax_prof,
        }
    try:
        return dict(row)
    except Exception:
        return row


def update_bus_status(bus_id: int, active: bool):
    """Update the active status of a bus and cascade to connected generators, loads, and shunts if deactivating."""
    # Get bus details to know grid_id and idtag
    bus_row = repo_get_bus_by_id(bus_id)
    if not bus_row:
        raise HTTPException(status_code=404, detail="Bus not found")
    
    # Extract grid_id and idtag from the row
    if isinstance(bus_row, tuple):
        grid_id = bus_row[1]  # grid_id is the second column
        bus_idtag = bus_row[2]  # idtag is the third column
    else:
        grid_id = bus_row.get('grid_id')
        bus_idtag = bus_row.get('idtag')
    
    # Update the bus status in database
    result = repo_update_bus_status(bus_id, active)
    if not result:
        raise HTTPException(status_code=404, detail="Bus not found")
    
    # If deactivating, cascade to connected elements in database
    if not active:
        repo_update_elements_by_bus_idtag(grid_id, bus_idtag, active)
    
    # Update VeraGrid circuit file
    try:
        tmp_path = get_tmp_file_path(grid_id)
        if tmp_path and os.path.exists(tmp_path):
            # Load the circuit
            circuit = gce.open_file(tmp_path)
            
            # Find the bus by idtag
            bus_found = None
            for bus in circuit.buses:
                if bus.idtag == bus_idtag:
                    bus_found = bus
                    break
            
            if bus_found:
                # Update bus status
                bus_found.active = active
                
                # If deactivating, cascade to connected generators, loads, and shunts
                # Note: Lines and transformers are NOT deactivated
                if not active:
                    # Deactivate generators connected to this bus
                    for gen in circuit.generators:
                        if gen.bus == bus_found:
                            gen.active = False
                    
                    # Deactivate loads connected to this bus
                    for load in circuit.loads:
                        if load.bus == bus_found:
                            load.active = False
                    
                    # Deactivate shunts connected to this bus
                    for shunt in circuit.shunts:
                        if shunt.bus == bus_found:
                            shunt.active = False
                
                # Save the updated circuit
                gce.save_file(circuit, tmp_path)
                logger.info(f"Updated bus {bus_idtag} status to {active} in VeraGrid circuit")
            else:
                logger.warning(f"Bus {bus_idtag} not found in VeraGrid circuit")
    except Exception as e:
        logger.error(f"Error updating VeraGrid circuit: {e}")
        # Don't fail the request if VeraGrid update fails
    
    return {"message": "Bus status updated", "bus_id": bus_id, "active": active}
