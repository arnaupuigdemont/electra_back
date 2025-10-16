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
        try:
            if 'tmp_path' in locals() and os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        # Log full stack trace for diagnosis
        logger.exception("Error during grid upload")
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {str(e)}")

def list_grid_ids():
    return repo_list_grid_ids()

# Delete a grid and its associated tmp file (if present)
def delete_grid(grid_id: int):
    import os
    from repositories.grids_repo import get_tmp_file_path, delete_grid as repo_delete_grid

    tmp_path = get_tmp_file_path(grid_id)
    # Delete DB row first (children cascade)
    repo_delete_grid(grid_id)
    # Best-effort removal of tmp file
    if tmp_path and os.path.exists(tmp_path):
        try:
            os.remove(tmp_path)
        except Exception:
            # Swallow file deletion errors; DB removal already done
            pass


# Bus-related logic lives in services/bus.py