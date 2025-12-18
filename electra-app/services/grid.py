from fastapi import UploadFile, HTTPException
import logging
import os
import tempfile
import VeraGridEngine as gce
from repositories.grid_ingest_repo import save_grid_payload
from repositories.grids_repo import list_grid_ids as repo_list_grid_ids

logger = logging.getLogger(__name__)


async def upload_file(file: UploadFile):
    try:

        # Es crea un fitxer temporal per guardar el contingut del fitxer pujat
        suffix = os.path.splitext(file.filename)[1]
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            content = await file.read()
            if not content:
                raise HTTPException(status_code=400, detail="Uploaded file is empty")
            tmp.write(content)
            tmp_path = tmp.name

        # S'utilitza VeraGrid per processar el fitxer i obtenir les dades del model
        grid_ = gce.open_file(tmp_path)
        instruction = gce.RemoteInstruction(operation=gce.SimulationTypes.NoSim)
        model_data = gce.gather_model_as_jsons_for_communication(circuit=grid_, instruction=instruction)

        # Basic validation of VeraGridEngine response
        if not isinstance(model_data, dict):
            raise HTTPException(
                status_code=400,
                detail=f"VeraGridEngine returned unexpected data type: {type(model_data).__name__}: {repr(model_data)[:200]}",
            )

        #src/VeraGrid/Gui/Diagrams/SchematicWidget/schematic_widget.py L1792
        #function auto_layout


        # Attach tmp file path so we can clean it up later on delete
        model_data["tmp_file_path"] = tmp_path

        # Persist in DB
        result = save_grid_payload(model_data)

        return {
            "message": "Grid saved",
            "grid_id": result["grid_id"],
            "buses_saved": result["buses_saved"],
            "lines_saved": result["lines_saved"],
            "generators_saved": result["generators_saved"],
            "loads_saved": result["loads_saved"],
            "shunts_saved": result["shunts_saved"],
            "transformers2w_saved": result["transformers2w_saved"],
        }

    except Exception as e:
        # Log full stack trace for diagnosis
        logger.exception("Error during grid upload")
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {str(e)}")

def list_grid_ids():
    return repo_list_grid_ids()

# Delete a grid and its associated tmp file (if present)
def delete_grid(grid_id: int):
    from repositories.grids_repo import get_tmp_file_path, delete_grid as repo_delete_grid

    tmp_path = get_tmp_file_path(grid_id)
    # Delete DB row first (children cascade)
    repo_delete_grid(grid_id)
    # Delete the temporary file
    if tmp_path and os.path.exists(tmp_path):
        try:
            os.remove(tmp_path)
            logger.info(f"Deleted temporary file: {tmp_path}")
        except Exception as e:
            logger.warning(f"Could not delete temporary file {tmp_path}: {e}")

# Calculate power flow for a grid
def calculate_power_flow(grid_id: int):
    from repositories.grids_repo import get_tmp_file_path
    from services.bus import list_buses as service_list_buses
    from services.generator import list_generators as service_list_generators
    from services.load import list_loads as service_list_loads
    from services.shunt import list_shunts as service_list_shunts
    from services.line import list_lines as service_list_lines
    from services.transformer2w import list_transformers2w as service_list_transformers2w
    
    # Get the temporary file path for this grid
    tmp_path = get_tmp_file_path(grid_id)
    
    if not tmp_path:
        raise HTTPException(status_code=404, detail=f"Grid {grid_id} not found")
    
    if not os.path.exists(tmp_path):
        raise HTTPException(status_code=404, detail=f"Grid file not found for grid {grid_id}")
    
    try:
        # Open the grid file
        main_circuit = gce.open_file(tmp_path)
        
        # Sync active status from database to circuit
        # Get all elements from database for this grid
        
        db_buses = service_list_buses()
        db_generators = service_list_generators()
        db_loads = service_list_loads()
        db_shunts = service_list_shunts()
        db_lines = service_list_lines()
        db_transformers = service_list_transformers2w()
        
        # Filter by grid_id
        db_buses = [b for b in db_buses if b.get('grid_id') == grid_id]
        db_generators = [g for g in db_generators if g.get('grid_id') == grid_id]
        db_loads = [l for l in db_loads if l.get('grid_id') == grid_id]
        db_shunts = [s for s in db_shunts if s.get('grid_id') == grid_id]
        db_lines = [l for l in db_lines if l.get('grid_id') == grid_id]
        db_transformers = [t for t in db_transformers if t.get('grid_id') == grid_id]
        
        # Sync buses
        for bus in main_circuit.buses:
            db_bus = next((b for b in db_buses if b.get('idtag') == bus.idtag), None)
            if db_bus and 'active' in db_bus:
                bus.active = db_bus['active']
        
        # Sync generators
        for gen in main_circuit.generators:
            db_gen = next((g for g in db_generators if g.get('idtag') == gen.idtag), None)
            if db_gen and 'active' in db_gen:
                gen.active = db_gen['active']
        
        # Sync loads
        for load in main_circuit.loads:
            db_load = next((l for l in db_loads if l.get('idtag') == load.idtag), None)
            if db_load and 'active' in db_load:
                load.active = db_load['active']
        
        # Sync shunts
        for shunt in main_circuit.shunts:
            db_shunt = next((s for s in db_shunts if s.get('idtag') == shunt.idtag), None)
            if db_shunt and 'active' in db_shunt:
                shunt.active = db_shunt['active']
        
        # Sync lines
        for line in main_circuit.lines:
            db_line = next((l for l in db_lines if l.get('idtag') == line.idtag), None)
            if db_line and 'active' in db_line:
                line.active = db_line['active']
        
        # Sync transformers
        for transformer in main_circuit.transformers2w:
            db_transformer = next((t for t in db_transformers if t.get('idtag') == transformer.idtag), None)
            if db_transformer and 'active' in db_transformer:
                transformer.active = db_transformer['active']
        
        # Save updated circuit back to tmp file
        gce.save_file(main_circuit, tmp_path)
        logger.info(f"Synced active status from database to circuit for grid {grid_id}")
        
        # Run power flow calculation
        results = gce.power_flow(main_circuit)
        
        # Convert DataFrames to JSON-serializable format
        import json
        import numpy as np
        
        # Helper to convert numpy types to native Python types
        def convert_numpy(obj):
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.bool_):
                return bool(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            return obj
        
        bus_df = results.get_bus_df()
        branch_df = results.get_branch_df()
        
        # Convert to dict and then convert numpy types
        bus_results = json.loads(bus_df.to_json(orient='records'))
        branch_results = json.loads(branch_df.to_json(orient='records'))
        
        # Return results as JSON
        # Note: branch_results includes both lines and transformers
        return {
            "grid_name": str(main_circuit.name),
            "converged": bool(results.converged),
            "error": float(results.error),
            "bus_results": bus_results,
            "branch_results": branch_results
        }
        
    except Exception as e:
        logger.exception(f"Error calculating power flow for grid {grid_id}")
        raise HTTPException(status_code=500, detail=f"Power flow calculation failed: {str(e)}")