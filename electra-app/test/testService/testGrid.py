import sys
import os

# Añadir el directorio raíz de la aplicación al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from fastapi import HTTPException, UploadFile
import io

from services.grid import upload_file, list_grid_ids, delete_grid, calculate_power_flow


# =====================================================================
# Tests para upload_file()
# =====================================================================

class TestUploadFile:
    """Tests para la función upload_file."""

    @pytest.mark.asyncio
    @patch("services.grid.save_grid_payload")
    @patch("services.grid.gce")
    @patch("services.grid.tempfile.NamedTemporaryFile")
    async def test_upload_file_success(self, mock_tempfile, mock_gce, mock_save):
        """Debe procesar y guardar un archivo correctamente."""
        # Mock del archivo temporal
        mock_tmp = MagicMock()
        mock_tmp.name = "/tmp/test.gridcal"
        mock_tmp.__enter__ = MagicMock(return_value=mock_tmp)
        mock_tmp.__exit__ = MagicMock(return_value=False)
        mock_tempfile.return_value = mock_tmp

        # Mock del archivo subido
        mock_file = MagicMock(spec=UploadFile)
        mock_file.filename = "test.gridcal"
        mock_file.read = AsyncMock(return_value=b"file content")

        # Mock de VeraGridEngine
        mock_circuit = MagicMock()
        mock_gce.open_file.return_value = mock_circuit
        mock_gce.RemoteInstruction.return_value = MagicMock()
        mock_gce.SimulationTypes.NoSim = "NoSim"
        mock_gce.gather_model_as_jsons_for_communication.return_value = {
            "buses": [],
            "lines": [],
        }

        # Mock del guardado
        mock_save.return_value = {
            "grid_id": 1,
            "buses_saved": 5,
            "lines_saved": 3,
            "generators_saved": 2,
            "loads_saved": 4,
            "shunts_saved": 1,
            "transformers2w_saved": 2,
        }

        result = await upload_file(mock_file)

        assert result["message"] == "Grid saved"
        assert result["grid_id"] == 1
        assert result["buses_saved"] == 5
        assert result["lines_saved"] == 3
        mock_gce.open_file.assert_called_once_with("/tmp/test.gridcal")

    @pytest.mark.asyncio
    async def test_upload_file_empty_file_raises_500(self):
        """Debe lanzar HTTPException 500 cuando el archivo está vacío (envuelve el 400 interno)."""
        mock_file = MagicMock(spec=UploadFile)
        mock_file.filename = "test.gridcal"
        mock_file.read = AsyncMock(return_value=b"")

        with pytest.raises(HTTPException) as exc_info:
            await upload_file(mock_file)

        # El código envuelve la HTTPException 400 en un 500
        assert exc_info.value.status_code == 500
        assert "empty" in exc_info.value.detail.lower()

    @pytest.mark.asyncio
    @patch("services.grid.gce")
    @patch("services.grid.tempfile.NamedTemporaryFile")
    async def test_upload_file_invalid_veragrid_response_raises_500(
        self, mock_tempfile, mock_gce
    ):
        """Debe lanzar 500 si VeraGridEngine devuelve tipo inesperado (envuelve el 400 interno)."""
        mock_tmp = MagicMock()
        mock_tmp.name = "/tmp/test.gridcal"
        mock_tmp.__enter__ = MagicMock(return_value=mock_tmp)
        mock_tmp.__exit__ = MagicMock(return_value=False)
        mock_tempfile.return_value = mock_tmp

        mock_file = MagicMock(spec=UploadFile)
        mock_file.filename = "test.gridcal"
        mock_file.read = AsyncMock(return_value=b"file content")

        mock_gce.open_file.return_value = MagicMock()
        mock_gce.RemoteInstruction.return_value = MagicMock()
        mock_gce.SimulationTypes.NoSim = "NoSim"
        # Devuelve string en lugar de dict
        mock_gce.gather_model_as_jsons_for_communication.return_value = "invalid"

        with pytest.raises(HTTPException) as exc_info:
            await upload_file(mock_file)

        # El código envuelve la HTTPException 400 en un 500
        assert exc_info.value.status_code == 500
        assert "unexpected data type" in exc_info.value.detail.lower()

    @pytest.mark.asyncio
    @patch("services.grid.gce")
    @patch("services.grid.tempfile.NamedTemporaryFile")
    async def test_upload_file_exception_raises_500(self, mock_tempfile, mock_gce):
        """Debe lanzar 500 si hay una excepción durante el procesamiento."""
        mock_tmp = MagicMock()
        mock_tmp.name = "/tmp/test.gridcal"
        mock_tmp.__enter__ = MagicMock(return_value=mock_tmp)
        mock_tmp.__exit__ = MagicMock(return_value=False)
        mock_tempfile.return_value = mock_tmp

        mock_file = MagicMock(spec=UploadFile)
        mock_file.filename = "test.gridcal"
        mock_file.read = AsyncMock(return_value=b"file content")

        mock_gce.open_file.side_effect = Exception("VeraGrid processing error")

        with pytest.raises(HTTPException) as exc_info:
            await upload_file(mock_file)

        assert exc_info.value.status_code == 500


# =====================================================================
# Tests para list_grid_ids()
# =====================================================================

class TestListGridIds:
    """Tests para la función list_grid_ids."""

    @patch("services.grid.repo_list_grid_ids")
    def test_list_grid_ids_returns_list(self, mock_repo):
        """Debe devolver la lista de grid_ids del repositorio."""
        mock_repo.return_value = [1, 2, 3]

        result = list_grid_ids()

        assert result == [1, 2, 3]
        mock_repo.assert_called_once()

    @patch("services.grid.repo_list_grid_ids")
    def test_list_grid_ids_returns_empty_list(self, mock_repo):
        """Debe devolver lista vacía cuando no hay grids."""
        mock_repo.return_value = []

        result = list_grid_ids()

        assert result == []
        mock_repo.assert_called_once()

    @patch("services.grid.repo_list_grid_ids")
    def test_list_grid_ids_returns_none(self, mock_repo):
        """Debe devolver None si el repositorio devuelve None."""
        mock_repo.return_value = None

        result = list_grid_ids()

        assert result is None


# =====================================================================
# Tests para delete_grid()
# =====================================================================

class TestDeleteGrid:
    """Tests para la función delete_grid."""

    @patch("services.grid.os.path.exists")
    @patch("services.grid.os.remove")
    @patch("repositories.grids_repo.delete_grid")
    @patch("repositories.grids_repo.get_tmp_file_path")
    def test_delete_grid_with_tmp_file(
        self, mock_get_tmp, mock_repo_delete, mock_remove, mock_exists
    ):
        """Debe eliminar el grid y su archivo temporal."""
        mock_get_tmp.return_value = "/tmp/test.gridcal"
        mock_exists.return_value = True

        delete_grid(1)

        mock_repo_delete.assert_called_once_with(1)
        mock_remove.assert_called_once_with("/tmp/test.gridcal")

    @patch("services.grid.os.path.exists")
    @patch("services.grid.os.remove")
    @patch("repositories.grids_repo.delete_grid")
    @patch("repositories.grids_repo.get_tmp_file_path")
    def test_delete_grid_without_tmp_file(
        self, mock_get_tmp, mock_repo_delete, mock_remove, mock_exists
    ):
        """Debe eliminar el grid aunque no haya archivo temporal."""
        mock_get_tmp.return_value = None

        delete_grid(1)

        mock_repo_delete.assert_called_once_with(1)
        mock_remove.assert_not_called()

    @patch("services.grid.os.path.exists")
    @patch("services.grid.os.remove")
    @patch("repositories.grids_repo.delete_grid")
    @patch("repositories.grids_repo.get_tmp_file_path")
    def test_delete_grid_tmp_file_not_exists(
        self, mock_get_tmp, mock_repo_delete, mock_remove, mock_exists
    ):
        """Debe eliminar el grid aunque el archivo no exista en disco."""
        mock_get_tmp.return_value = "/tmp/test.gridcal"
        mock_exists.return_value = False

        delete_grid(1)

        mock_repo_delete.assert_called_once_with(1)
        mock_remove.assert_not_called()

    @patch("services.grid.os.path.exists")
    @patch("services.grid.os.remove")
    @patch("repositories.grids_repo.delete_grid")
    @patch("repositories.grids_repo.get_tmp_file_path")
    def test_delete_grid_remove_file_fails_continues(
        self, mock_get_tmp, mock_repo_delete, mock_remove, mock_exists
    ):
        """Debe continuar si falla al eliminar el archivo temporal."""
        mock_get_tmp.return_value = "/tmp/test.gridcal"
        mock_exists.return_value = True
        mock_remove.side_effect = Exception("Permission denied")

        # No debe lanzar excepción
        delete_grid(1)

        mock_repo_delete.assert_called_once_with(1)


# =====================================================================
# Tests para calculate_power_flow()
# =====================================================================

class TestCalculatePowerFlow:
    """Tests para la función calculate_power_flow."""

    @patch("repositories.grids_repo.get_tmp_file_path")
    def test_calculate_power_flow_grid_not_found_raises_404(self, mock_get_tmp):
        """Debe lanzar 404 si el grid no existe."""
        mock_get_tmp.return_value = None

        with pytest.raises(HTTPException) as exc_info:
            calculate_power_flow(999)

        assert exc_info.value.status_code == 404
        assert "not found" in exc_info.value.detail.lower()

    @patch("services.grid.os.path.exists")
    @patch("repositories.grids_repo.get_tmp_file_path")
    def test_calculate_power_flow_file_not_exists_raises_404(
        self, mock_get_tmp, mock_exists
    ):
        """Debe lanzar 404 si el archivo del grid no existe."""
        mock_get_tmp.return_value = "/tmp/test.gridcal"
        mock_exists.return_value = False

        with pytest.raises(HTTPException) as exc_info:
            calculate_power_flow(1)

        assert exc_info.value.status_code == 404
        assert "file not found" in exc_info.value.detail.lower()

    @patch("services.grid.gce")
    @patch("services.transformer2w.repo_list_transformers2w")
    @patch("services.line.repo_list_lines")
    @patch("services.shunt.repo_list_shunts")
    @patch("services.load.repo_list_loads")
    @patch("services.generator.repo_list_generators")
    @patch("services.bus.repo_list_buses")
    @patch("services.grid.os.path.exists")
    @patch("repositories.grids_repo.get_tmp_file_path")
    def test_calculate_power_flow_success(
        self, mock_get_tmp, mock_exists, mock_buses, mock_generators,
        mock_loads, mock_shunts, mock_lines, mock_transformers, mock_gce
    ):
        """Debe calcular power flow correctamente."""
        mock_get_tmp.return_value = "/tmp/test.gridcal"
        mock_exists.return_value = True

        # Mock servicios de DB vacíos (sin elementos)
        mock_buses.return_value = []
        mock_generators.return_value = []
        mock_loads.return_value = []
        mock_shunts.return_value = []
        mock_lines.return_value = []
        mock_transformers.return_value = []

        # Mock del circuito
        mock_circuit = MagicMock()
        mock_circuit.name = "Test Circuit"
        mock_circuit.buses = []
        mock_circuit.generators = []
        mock_circuit.loads = []
        mock_circuit.shunts = []
        mock_circuit.lines = []
        mock_circuit.transformers2w = []

        mock_gce.open_file.return_value = mock_circuit

        # Mock de resultados de power flow
        mock_results = MagicMock()
        mock_results.converged = True
        mock_results.error = 0.0001

        # Mock de DataFrames
        mock_bus_df = MagicMock()
        mock_bus_df.to_json.return_value = "[]"
        mock_branch_df = MagicMock()
        mock_branch_df.to_json.return_value = "[]"

        mock_results.get_bus_df.return_value = mock_bus_df
        mock_results.get_branch_df.return_value = mock_branch_df

        mock_gce.power_flow.return_value = mock_results

        result = calculate_power_flow(1)

        assert result["grid_name"] == "Test Circuit"
        assert result["converged"] is True
        assert result["error"] == 0.0001
        assert "bus_results" in result
        assert "branch_results" in result
        mock_gce.save_file.assert_called_once()

    @patch("services.grid.gce")
    @patch("services.transformer2w.repo_list_transformers2w")
    @patch("services.line.repo_list_lines")
    @patch("services.shunt.repo_list_shunts")
    @patch("services.load.repo_list_loads")
    @patch("services.generator.repo_list_generators")
    @patch("services.bus.repo_list_buses")
    @patch("services.grid.os.path.exists")
    @patch("repositories.grids_repo.get_tmp_file_path")
    def test_calculate_power_flow_syncs_active_status(
        self, mock_get_tmp, mock_exists, mock_buses, mock_generators,
        mock_loads, mock_shunts, mock_lines, mock_transformers, mock_gce
    ):
        """Debe sincronizar el estado activo de los elementos desde la DB."""
        mock_get_tmp.return_value = "/tmp/test.gridcal"
        mock_exists.return_value = True

        # Mock bus en DB con active=False
        mock_buses.return_value = [
            {"grid_id": 1, "idtag": "bus-001", "active": False}
        ]
        mock_generators.return_value = []
        mock_loads.return_value = []
        mock_shunts.return_value = []
        mock_lines.return_value = []
        mock_transformers.return_value = []

        # Mock del circuito con bus activo
        mock_bus = MagicMock()
        mock_bus.idtag = "bus-001"
        mock_bus.active = True

        mock_circuit = MagicMock()
        mock_circuit.name = "Test Circuit"
        mock_circuit.buses = [mock_bus]
        mock_circuit.generators = []
        mock_circuit.loads = []
        mock_circuit.shunts = []
        mock_circuit.lines = []
        mock_circuit.transformers2w = []

        mock_gce.open_file.return_value = mock_circuit

        # Mock de resultados
        mock_results = MagicMock()
        mock_results.converged = True
        mock_results.error = 0.0001
        mock_bus_df = MagicMock()
        mock_bus_df.to_json.return_value = "[]"
        mock_branch_df = MagicMock()
        mock_branch_df.to_json.return_value = "[]"
        mock_results.get_bus_df.return_value = mock_bus_df
        mock_results.get_branch_df.return_value = mock_branch_df
        mock_gce.power_flow.return_value = mock_results

        calculate_power_flow(1)

        # Verificar que el bus fue sincronizado a inactive
        assert mock_bus.active is False

    @patch("services.grid.gce")
    @patch("services.transformer2w.repo_list_transformers2w")
    @patch("services.line.repo_list_lines")
    @patch("services.shunt.repo_list_shunts")
    @patch("services.load.repo_list_loads")
    @patch("services.generator.repo_list_generators")
    @patch("services.bus.repo_list_buses")
    @patch("services.grid.os.path.exists")
    @patch("repositories.grids_repo.get_tmp_file_path")
    def test_calculate_power_flow_exception_raises_500(
        self, mock_get_tmp, mock_exists, mock_buses, mock_generators,
        mock_loads, mock_shunts, mock_lines, mock_transformers, mock_gce
    ):
        """Debe lanzar 500 si hay una excepción durante el cálculo."""
        mock_get_tmp.return_value = "/tmp/test.gridcal"
        mock_exists.return_value = True

        mock_buses.return_value = []
        mock_generators.return_value = []
        mock_loads.return_value = []
        mock_shunts.return_value = []
        mock_lines.return_value = []
        mock_transformers.return_value = []

        mock_gce.open_file.side_effect = Exception("Power flow error")

        with pytest.raises(HTTPException) as exc_info:
            calculate_power_flow(1)

        assert exc_info.value.status_code == 500
        assert "failed" in exc_info.value.detail.lower()

    @patch("services.grid.gce")
    @patch("services.transformer2w.repo_list_transformers2w")
    @patch("services.line.repo_list_lines")
    @patch("services.shunt.repo_list_shunts")
    @patch("services.load.repo_list_loads")
    @patch("services.generator.repo_list_generators")
    @patch("services.bus.repo_list_buses")
    @patch("services.grid.os.path.exists")
    @patch("repositories.grids_repo.get_tmp_file_path")
    def test_calculate_power_flow_filters_by_grid_id(
        self, mock_get_tmp, mock_exists, mock_buses, mock_generators,
        mock_loads, mock_shunts, mock_lines, mock_transformers, mock_gce
    ):
        """Debe filtrar elementos por grid_id."""
        mock_get_tmp.return_value = "/tmp/test.gridcal"
        mock_exists.return_value = True

        # Elementos de distintos grids
        mock_buses.return_value = [
            {"grid_id": 1, "idtag": "bus-001", "active": False},
            {"grid_id": 2, "idtag": "bus-002", "active": True},  # Otro grid
        ]
        mock_generators.return_value = []
        mock_loads.return_value = []
        mock_shunts.return_value = []
        mock_lines.return_value = []
        mock_transformers.return_value = []

        # Mock del circuito
        mock_bus1 = MagicMock()
        mock_bus1.idtag = "bus-001"
        mock_bus1.active = True

        mock_bus2 = MagicMock()
        mock_bus2.idtag = "bus-002"
        mock_bus2.active = True

        mock_circuit = MagicMock()
        mock_circuit.name = "Test Circuit"
        mock_circuit.buses = [mock_bus1, mock_bus2]
        mock_circuit.generators = []
        mock_circuit.loads = []
        mock_circuit.shunts = []
        mock_circuit.lines = []
        mock_circuit.transformers2w = []

        mock_gce.open_file.return_value = mock_circuit

        # Mock de resultados
        mock_results = MagicMock()
        mock_results.converged = True
        mock_results.error = 0.0001
        mock_bus_df = MagicMock()
        mock_bus_df.to_json.return_value = "[]"
        mock_branch_df = MagicMock()
        mock_branch_df.to_json.return_value = "[]"
        mock_results.get_bus_df.return_value = mock_bus_df
        mock_results.get_branch_df.return_value = mock_branch_df
        mock_gce.power_flow.return_value = mock_results

        calculate_power_flow(1)

        # Solo bus-001 debe ser sincronizado (grid_id=1), bus-002 no (grid_id=2)
        assert mock_bus1.active is False
        # bus-002 no tiene match en DB filtrada, así que mantiene su valor original
        assert mock_bus2.active is True
