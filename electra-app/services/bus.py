from fastapi import HTTPException
from repositories.buses_repo import (
    get_bus_by_id as repo_get_bus_by_id,
    list_buses as repo_list_buses,
)

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
