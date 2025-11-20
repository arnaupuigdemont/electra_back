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
    
    # Get the temporary file path for this grid
    tmp_path = get_tmp_file_path(grid_id)
    
    if not tmp_path:
        raise HTTPException(status_code=404, detail=f"Grid {grid_id} not found")
    
    if not os.path.exists(tmp_path):
        raise HTTPException(status_code=404, detail=f"Grid file not found for grid {grid_id}")
    
    try:
        # Open the grid file
        main_circuit = gce.open_file(tmp_path)
        
        # Run power flow calculation
        results = gce.power_flow(main_circuit)
        
        # Print results to console
        print(f"\n===== Power Flow Results for {main_circuit.name} =====")
        print(f"Converged: {results.converged}, Error: {results.error}")
        print("\n--- Bus Results ---")
        print(results.get_bus_df())
        print("\n--- Branch Results ---")
        print(results.get_branch_df())
        print("=" * 50 + "\n")
        
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