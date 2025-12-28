import sys
import os

# Añadir el directorio raíz de la aplicación al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from fastapi import HTTPException
from fastapi.testclient import TestClient
from io import BytesIO

from main import get_app


# =====================================================================
# Fixtures y datos de prueba
# =====================================================================

@pytest.fixture
def client():
    """Cliente de prueba para la API FastAPI."""
    app = get_app()
    return TestClient(app)


@pytest.fixture
def sample_grid_ids():
    """Lista de IDs de grids."""
    return [1, 2, 3]


@pytest.fixture
def sample_upload_result():
    """Resultado de subida de archivo."""
    return {
        "grid_id": 1,
        "filename": "test_grid.gridcal",
        "message": "Archivo subido correctamente"
    }


@pytest.fixture
def sample_power_flow_result():
    """Resultado de cálculo de power flow."""
    return {
        "grid_id": 1,
        "converged": True,
        "iterations": 5,
        "error": 0.0001,
        "buses": [
            {"id": 1, "vm": 1.0, "va": 0.0},
            {"id": 2, "vm": 0.98, "va": -2.5}
        ]
    }


# =====================================================================
# Tests para POST /grid/files/upload (upload_file)
# =====================================================================

class TestUploadFile:
    """Tests para el endpoint POST /grid/files/upload"""

    @patch("routes.grid.grid.upload_file")
    def test_upload_file_success(self, mock_upload, client, sample_upload_result):
        """Debe subir un archivo correctamente."""
        mock_upload.return_value = sample_upload_result
        
        file_content = b"fake gridcal content"
        files = {"file": ("test_grid.gridcal", BytesIO(file_content), "application/octet-stream")}
        
        response = client.post("/grid/files/upload", files=files)
        
        assert response.status_code == 200
        data = response.json()
        assert data["grid_id"] == 1
        assert data["filename"] == "test_grid.gridcal"
        mock_upload.assert_called_once()

    @patch("routes.grid.grid.upload_file")
    def test_upload_file_service_error(self, mock_upload, client):
        """Debe propagar el error HTTP cuando el servicio falla."""
        mock_upload.side_effect = HTTPException(status_code=500, detail="Error procesando archivo")
        
        file_content = b"fake gridcal content"
        files = {"file": ("test_grid.gridcal", BytesIO(file_content), "application/octet-stream")}
        
        response = client.post("/grid/files/upload", files=files)
        
        assert response.status_code == 500
        mock_upload.assert_called_once()

    def test_upload_file_missing_file(self, client):
        """Debe devolver error cuando no se proporciona archivo."""
        response = client.post("/grid/files/upload")
        
        assert response.status_code == 422  # Validation error

    @patch("routes.grid.grid.upload_file")
    def test_upload_file_invalid_format(self, mock_upload, client):
        """Debe propagar error cuando el formato es inválido."""
        mock_upload.side_effect = HTTPException(status_code=400, detail="Formato de archivo no soportado")
        
        file_content = b"invalid content"
        files = {"file": ("test.txt", BytesIO(file_content), "text/plain")}
        
        response = client.post("/grid/files/upload", files=files)
        
        assert response.status_code == 400
        mock_upload.assert_called_once()


# =====================================================================
# Tests para GET /grid/ids (get_grid_ids)
# =====================================================================

class TestGetGridIds:
    """Tests para el endpoint GET /grid/ids"""

    @patch("routes.grid.grid.list_grid_ids")
    def test_get_grid_ids_returns_list(self, mock_list_ids, client, sample_grid_ids):
        """Debe devolver una lista de IDs de grids."""
        mock_list_ids.return_value = sample_grid_ids
        
        response = client.get("/grid/ids")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 3
        assert data == [1, 2, 3]
        mock_list_ids.assert_called_once()

    @patch("routes.grid.grid.list_grid_ids")
    def test_get_grid_ids_returns_empty_list(self, mock_list_ids, client):
        """Debe devolver una lista vacía cuando no hay grids."""
        mock_list_ids.return_value = []
        
        response = client.get("/grid/ids")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 0
        mock_list_ids.assert_called_once()

    @patch("routes.grid.grid.list_grid_ids")
    def test_get_grid_ids_service_error(self, mock_list_ids, client):
        """Debe propagar el error HTTP cuando el servicio falla."""
        mock_list_ids.side_effect = HTTPException(status_code=500, detail="Error interno del servidor")
        
        response = client.get("/grid/ids")
        
        assert response.status_code == 500
        mock_list_ids.assert_called_once()


# =====================================================================
# Tests para DELETE /grid/{grid_id} (delete_grid)
# =====================================================================

class TestDeleteGrid:
    """Tests para el endpoint DELETE /grid/{grid_id}"""

    @patch("routes.grid.grid.delete_grid")
    def test_delete_grid_success(self, mock_delete, client):
        """Debe eliminar un grid correctamente."""
        mock_delete.return_value = None
        
        response = client.delete("/grid/1")
        
        assert response.status_code == 200
        data = response.json()
        assert data["deleted"] is True
        assert data["grid_id"] == 1
        mock_delete.assert_called_once_with(1)

    @patch("routes.grid.grid.delete_grid")
    def test_delete_grid_not_found(self, mock_delete, client):
        """Debe devolver 404 cuando el grid no existe."""
        mock_delete.side_effect = HTTPException(status_code=404, detail="Grid no encontrado")
        
        response = client.delete("/grid/999")
        
        assert response.status_code == 404
        mock_delete.assert_called_once_with(999)

    @patch("routes.grid.grid.delete_grid")
    def test_delete_grid_different_ids(self, mock_delete, client):
        """Debe pasar el ID correcto al servicio."""
        mock_delete.return_value = None
        
        response = client.delete("/grid/42")
        
        assert response.status_code == 200
        assert response.json()["grid_id"] == 42
        mock_delete.assert_called_once_with(42)

    def test_delete_grid_invalid_id_type(self, client):
        """Debe devolver error de validación para ID no numérico."""
        response = client.delete("/grid/abc")
        
        assert response.status_code == 422  # Validation error

    @patch("routes.grid.grid.delete_grid")
    def test_delete_grid_service_error(self, mock_delete, client):
        """Debe propagar el error HTTP cuando el servicio falla."""
        mock_delete.side_effect = HTTPException(status_code=500, detail="Error interno")
        
        response = client.delete("/grid/1")
        
        assert response.status_code == 500
        mock_delete.assert_called_once_with(1)


# =====================================================================
# Tests para GET /grid/{grid_id}/power-flow (calculate_power_flow)
# =====================================================================

class TestCalculatePowerFlow:
    """Tests para el endpoint GET /grid/{grid_id}/power-flow"""

    @patch("routes.grid.grid.calculate_power_flow")
    def test_calculate_power_flow_success(self, mock_power_flow, client, sample_power_flow_result):
        """Debe calcular el power flow correctamente."""
        mock_power_flow.return_value = sample_power_flow_result
        
        response = client.get("/grid/1/power-flow")
        
        assert response.status_code == 200
        data = response.json()
        assert data["grid_id"] == 1
        assert data["converged"] is True
        assert "buses" in data
        mock_power_flow.assert_called_once_with(1)

    @patch("routes.grid.grid.calculate_power_flow")
    def test_calculate_power_flow_not_found(self, mock_power_flow, client):
        """Debe devolver 404 cuando el grid no existe."""
        mock_power_flow.side_effect = HTTPException(status_code=404, detail="Grid no encontrado")
        
        response = client.get("/grid/999/power-flow")
        
        assert response.status_code == 404
        mock_power_flow.assert_called_once_with(999)

    @patch("routes.grid.grid.calculate_power_flow")
    def test_calculate_power_flow_different_ids(self, mock_power_flow, client, sample_power_flow_result):
        """Debe pasar el ID correcto al servicio."""
        sample_power_flow_result["grid_id"] = 42
        mock_power_flow.return_value = sample_power_flow_result
        
        response = client.get("/grid/42/power-flow")
        
        assert response.status_code == 200
        assert response.json()["grid_id"] == 42
        mock_power_flow.assert_called_once_with(42)

    def test_calculate_power_flow_invalid_id_type(self, client):
        """Debe devolver error de validación para ID no numérico."""
        response = client.get("/grid/abc/power-flow")
        
        assert response.status_code == 422  # Validation error

    @patch("routes.grid.grid.calculate_power_flow")
    def test_calculate_power_flow_service_error(self, mock_power_flow, client):
        """Debe propagar el error HTTP cuando el servicio falla."""
        mock_power_flow.side_effect = HTTPException(status_code=500, detail="Error en cálculo")
        
        response = client.get("/grid/1/power-flow")
        
        assert response.status_code == 500
        mock_power_flow.assert_called_once_with(1)

    @patch("routes.grid.grid.calculate_power_flow")
    def test_calculate_power_flow_not_converged(self, mock_power_flow, client):
        """Debe devolver resultado cuando el power flow no converge."""
        mock_power_flow.return_value = {
            "grid_id": 1,
            "converged": False,
            "iterations": 100,
            "error": 0.5
        }
        
        response = client.get("/grid/1/power-flow")
        
        assert response.status_code == 200
        data = response.json()
        assert data["converged"] is False
        mock_power_flow.assert_called_once_with(1)
