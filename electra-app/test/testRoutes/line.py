import sys
import os

# Añadir el directorio raíz de la aplicación al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import pytest
from unittest.mock import patch, MagicMock
from fastapi import HTTPException
from fastapi.testclient import TestClient

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
def sample_line_dict():
    """Diccionario que simula una línea devuelta por el servicio."""
    return {
        "id": 1,
        "grid_id": 1,
        "idtag": "line-001",
        "name": "Line 1",
        "code": "L001",
        "bus_from_id": 1,
        "bus_to_id": 2,
        "active": True,
        "rate": 100.0,
        "r": 0.01,
        "x": 0.1,
        "b": 0.02,
        "length": 50.0,
        "contingency_factor": 1.0,
        "overload_cost": 1000.0,
        "mttf": 8760.0,
        "mttr": 24.0,
    }


@pytest.fixture
def sample_lines_list(sample_line_dict):
    """Lista de líneas para probar list_lines."""
    line2 = sample_line_dict.copy()
    line2["id"] = 2
    line2["idtag"] = "line-002"
    line2["name"] = "Line 2"
    return [sample_line_dict, line2]


# =====================================================================
# Tests para GET /line/ (list_lines)
# =====================================================================

class TestListLines:
    """Tests para el endpoint GET /line/"""

    @patch("routes.line.list_lines")
    def test_list_lines_returns_list(self, mock_list_lines, client, sample_lines_list):
        """Debe devolver una lista de líneas cuando hay datos."""
        mock_list_lines.return_value = sample_lines_list
        
        response = client.get("/line/")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 2
        assert data[0]["id"] == 1
        assert data[1]["id"] == 2
        mock_list_lines.assert_called_once()

    @patch("routes.line.list_lines")
    def test_list_lines_returns_empty_list(self, mock_list_lines, client):
        """Debe devolver una lista vacía cuando no hay líneas."""
        mock_list_lines.return_value = []
        
        response = client.get("/line/")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 0
        mock_list_lines.assert_called_once()

    @patch("routes.line.list_lines")
    def test_list_lines_service_error(self, mock_list_lines, client):
        """Debe propagar el error HTTP cuando el servicio falla."""
        mock_list_lines.side_effect = HTTPException(status_code=500, detail="Error interno del servidor")
        
        response = client.get("/line/")
        
        assert response.status_code == 500
        mock_list_lines.assert_called_once()


# =====================================================================
# Tests para GET /line/{line_id} (get_line)
# =====================================================================

class TestGetLine:
    """Tests para el endpoint GET /line/{line_id}"""

    @patch("routes.line.get_line")
    def test_get_line_success(self, mock_get_line, client, sample_line_dict):
        """Debe devolver una línea cuando existe."""
        mock_get_line.return_value = sample_line_dict
        
        response = client.get("/line/1")
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == 1
        assert data["name"] == "Line 1"
        assert data["idtag"] == "line-001"
        mock_get_line.assert_called_once_with(1)

    @patch("routes.line.get_line")
    def test_get_line_not_found(self, mock_get_line, client):
        """Debe devolver 404 cuando la línea no existe."""
        mock_get_line.side_effect = HTTPException(status_code=404, detail="Línea no encontrada")
        
        response = client.get("/line/999")
        
        assert response.status_code == 404
        mock_get_line.assert_called_once_with(999)

    @patch("routes.line.get_line")
    def test_get_line_different_ids(self, mock_get_line, client, sample_line_dict):
        """Debe pasar el ID correcto al servicio."""
        sample_line_dict["id"] = 42
        mock_get_line.return_value = sample_line_dict
        
        response = client.get("/line/42")
        
        assert response.status_code == 200
        assert response.json()["id"] == 42
        mock_get_line.assert_called_once_with(42)

    def test_get_line_invalid_id_type(self, client):
        """Debe devolver error de validación para ID no numérico."""
        response = client.get("/line/abc")
        
        assert response.status_code == 422  # Validation error

    @patch("routes.line.get_line")
    def test_get_line_service_error(self, mock_get_line, client):
        """Debe propagar el error HTTP cuando el servicio falla."""
        mock_get_line.side_effect = HTTPException(status_code=500, detail="Error interno")
        
        response = client.get("/line/1")
        
        assert response.status_code == 500
        mock_get_line.assert_called_once_with(1)


# =====================================================================
# Tests para PATCH /line/{line_id}/status (update_line_status)
# =====================================================================

class TestUpdateLineStatus:
    """Tests para el endpoint PATCH /line/{line_id}/status"""

    @patch("routes.line.update_line_status")
    def test_update_line_status_activate(self, mock_update_status, client, sample_line_dict):
        """Debe activar una línea correctamente."""
        sample_line_dict["active"] = True
        mock_update_status.return_value = sample_line_dict
        
        response = client.patch("/line/1/status", json={"active": True})
        
        assert response.status_code == 200
        assert response.json()["active"] is True
        mock_update_status.assert_called_once_with(1, True)

    @patch("routes.line.update_line_status")
    def test_update_line_status_deactivate(self, mock_update_status, client, sample_line_dict):
        """Debe desactivar una línea correctamente."""
        sample_line_dict["active"] = False
        mock_update_status.return_value = sample_line_dict
        
        response = client.patch("/line/1/status", json={"active": False})
        
        assert response.status_code == 200
        assert response.json()["active"] is False
        mock_update_status.assert_called_once_with(1, False)

    @patch("routes.line.update_line_status")
    def test_update_line_status_not_found(self, mock_update_status, client):
        """Debe devolver 404 cuando la línea no existe."""
        mock_update_status.side_effect = HTTPException(status_code=404, detail="Línea no encontrada")
        
        response = client.patch("/line/999/status", json={"active": True})
        
        assert response.status_code == 404
        mock_update_status.assert_called_once_with(999, True)

    def test_update_line_status_missing_body(self, client):
        """Debe devolver error de validación cuando falta el body."""
        response = client.patch("/line/1/status")
        
        assert response.status_code == 422  # Validation error

    def test_update_line_status_missing_active_field(self, client):
        """Debe devolver error de validación cuando falta el campo active."""
        response = client.patch("/line/1/status", json={})
        
        assert response.status_code == 422  # Validation error

    def test_update_line_status_invalid_active_type(self, client):
        """Debe devolver error de validación cuando active no es un tipo válido."""
        # Nota: Pydantic coerce strings como "yes"/"true" a True, por lo que usamos un objeto
        response = client.patch("/line/1/status", json={"active": {"invalid": "type"}})
        
        assert response.status_code == 422  # Validation error

    def test_update_line_status_invalid_id_type(self, client):
        """Debe devolver error de validación para ID no numérico."""
        response = client.patch("/line/abc/status", json={"active": True})
        
        assert response.status_code == 422  # Validation error

    @patch("routes.line.update_line_status")
    def test_update_line_status_service_error(self, mock_update_status, client):
        """Debe propagar el error HTTP cuando el servicio falla."""
        mock_update_status.side_effect = HTTPException(status_code=500, detail="Error interno")
        
        response = client.patch("/line/1/status", json={"active": True})
        
        assert response.status_code == 500
        mock_update_status.assert_called_once_with(1, True)

    @patch("routes.line.update_line_status")
    def test_update_line_status_returns_updated_line(self, mock_update_status, client, sample_line_dict):
        """Debe devolver la línea actualizada con todos sus campos."""
        mock_update_status.return_value = sample_line_dict
        
        response = client.patch("/line/1/status", json={"active": True})
        
        assert response.status_code == 200
        data = response.json()
        assert "id" in data
        assert "name" in data
        assert "active" in data
        assert "r" in data
        assert "x" in data
        mock_update_status.assert_called_once_with(1, True)
