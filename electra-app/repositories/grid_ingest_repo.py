import json
from typing import Dict, Any
from db.db import get_conn
from . import grids_repo, buses_repo, loads_repo, generators_repo, shunts_repo, transformers2w_repo, lines_repo


def _ensure_schema(conn) -> None:
    grids_repo.ensure_schema(conn)
    buses_repo.ensure_schema(conn)
    loads_repo.ensure_schema(conn)
    generators_repo.ensure_schema(conn)
    shunts_repo.ensure_schema(conn)
    transformers2w_repo.ensure_schema(conn)
    lines_repo.ensure_schema(conn)


def save_grid_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    conn = get_conn()
    try:
        _ensure_schema(conn)

        name = payload.get("name")
        base_mva = payload.get("baseMVA")
        raw_json = json.dumps(payload)

        # The payload may include a tmp_file_path field to be stored alongside the grid
        tmp_file_path = payload.get("tmp_file_path")
        grid_id = grids_repo.insert_grid(name, base_mva, raw_json, tmp_file_path)

        md = (payload.get("model_data") or {})

        # Buses
        buses = md.get("bus") or []
        buses_saved = 0
        if buses:
            buses_saved = buses_repo.upsert(conn, grid_id, buses)

        # Loads
        loads = md.get("load") or []
        loads_saved = 0
        if loads:
            loads_saved = loads_repo.upsert(conn, grid_id, loads)

        # Generators
        generators = md.get("generator") or []
        generators_saved = 0
        if generators:
            generators_saved = generators_repo.upsert(conn, grid_id, generators)

        # Shunts
        shunts = md.get("shunt") or []
        shunts_saved = 0
        if shunts:
            shunts_saved = shunts_repo.upsert(conn, grid_id, shunts)

        # Transformers 2W
        trafos = md.get("transformer2w") or []
        transformers2w_saved = 0
        if trafos:
            transformers2w_saved = transformers2w_repo.upsert(conn, grid_id, trafos)

        # Lines
        lines = md.get("line") or []
        lines_saved = 0
        if lines:
            lines_saved = lines_repo.upsert(conn, grid_id, lines)

        conn.commit()
        return {
            "grid_id": grid_id,
            "buses_saved": buses_saved,
            "loads_saved": loads_saved,
            "generators_saved": generators_saved,
            "shunts_saved": shunts_saved,
            "transformers2w_saved": transformers2w_saved,
            "lines_saved": lines_saved,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
