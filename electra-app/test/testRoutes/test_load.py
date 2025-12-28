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
def sample_load_dict():
    """Diccionario que simula una carga devuelta por el servicio."""
    return {
        "id": 1,
        "grid_id": 1,
        "idtag": "load-001",
        "name": "Load 1",
        "code": "LD001",
        "bus_id": 1,
        "active": True,
        "p": 50.0,
        "q": 20.0,
        "cost": 100.0,
        "mttf": 8760.0,
        "mttr": 24.0,
    }


@pytest.fixture
def sample_loads_list(sample_load_dict):
    """Lista de cargas para probar list_loads."""
    load2 = sample_load_dict.copy()
    load2["id"] = 2
    load2["idtag"] = "load-002"
    load2["name"] = "Load 2"
    return [sample_load_dict, load2]


# =====================================================================
# Tests para GET /load/ (list_loads)
# =====================================================================

class TestListLoads:
    """Tests para el endpoint GET /load/"""

    @patch("routes.load.list_loads")
    def test_list_loads_returns_list(self, mock_list_loads, client, sample_loads_list):
        """Debe devolver una lista de cargas cuando hay datos."""
        mock_list_loads.return_value = sample_loads_list
        
        response = client.get("/load/")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 2
        assert data[0]["id"] == 1
        assert data[1]["id"] == 2
        mock_list_loads.assert_called_once()

    @patch("routes.load.list_loads")
    def test_list_loads_returns_empty_list(self, mock_list_loads, client):
        """Debe devolver una lista vacía cuando no hay cargas."""
        mock_list_loads.return_value = []
        
        response = client.get("/load/")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 0
        mock_list_loads.assert_called_once()

    @patch("routes.load.list_loads")
    def test_list_loads_service_error(self, mock_list_loads, client):
        """Debe propagar el error HTTP cuando el servicio falla."""
        mock_list_loads.side_effect = HTTPException(status_code=500, detail="Error interno del servidor")
        
        response = client.get("/load/")
        
        assert response.status_code == 500
        mock_list_loads.assert_called_once()


# =====================================================================
# Tests para GET /load/{load_id} (get_load)
# =====================================================================

class TestGetLoad:
    """Tests para el endpoint GET /load/{load_id}"""

    @patch("routes.load.get_load")
    def test_get_load_success(self, mock_get_load, client, sample_load_dict):
        """Debe devolver una carga cuando existe."""
        mock_get_load.return_value = sample_load_dict
        
        response = client.get("/load/1")
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == 1
        assert data["name"] == "Load 1"
        assert data["idtag"] == "load-001"
        mock_get_load.assert_called_once_with(1)

    @patch("routes.load.get_load")
    def test_get_load_not_found(self, mock_get_load, client):
        """Debe devolver 404 cuando la carga no existe."""
        mock_get_load.side_effect = HTTPException(status_code=404, detail="Carga no encontrada")
        
        response = client.get("/load/999")
        
        assert response.status_code == 404
        mock_get_load.assert_called_once_with(999)

    @patch("routes.load.get_load")
    def test_get_load_different_ids(self, mock_get_load, client, sample_load_dict):
        """Debe pasar el ID correcto al servicio."""
        sample_load_dict["id"] = 42
        mock_get_load.return_value = sample_load_dict
        
        response = client.get("/load/42")
        
        assert response.status_code == 200
        assert response.json()["id"] == 42
        mock_get_load.assert_called_once_with(42)

    def test_get_load_invalid_id_type(self, client):
        """Debe devolver error de validación para ID no numérico."""
        response = client.get("/load/abc")
        
        assert response.status_code == 422  # Validation error

    @patch("routes.load.get_load")
    def test_get_load_service_error(self, mock_get_load, client):
        """Debe propagar el error HTTP cuando el servicio falla."""
        mock_get_load.side_effect = HTTPException(status_code=500, detail="Error interno")
        
        response = client.get("/load/1")
        
        assert response.status_code == 500
        mock_get_load.assert_called_once_with(1)


# =====================================================================
# Tests para PATCH /load/{load_id}/status (update_load_status)
# =====================================================================

class TestUpdateLoadStatus:
    """Tests para el endpoint PATCH /load/{load_id}/status"""

    @patch("routes.load.update_load_status")
    def test_update_load_status_activate(self, mock_update_status, client, sample_load_dict):
        """Debe activar una carga correctamente."""
        sample_load_dict["active"] = True
        mock_update_status.return_value = sample_load_dict
        
        response = client.patch("/load/1/status", json={"active": True})
        
        assert response.status_code == 200
        assert response.json()["active"] is True
        mock_update_status.assert_called_once_with(1, True)

    @patch("routes.load.update_load_status")
    def test_update_load_status_deactivate(self, mock_update_status, client, sample_load_dict):
        """Debe desactivar una carga correctamente."""
        sample_load_dict["active"] = False
        mock_update_status.return_value = sample_load_dict
        
        response = client.patch("/load/1/status", json={"active": False})
        
        assert response.status_code == 200
        assert response.json()["active"] is False
        mock_update_status.assert_called_once_with(1, False)

    @patch("routes.load.update_load_status")
    def test_update_load_status_not_found(self, mock_update_status, client):
        """Debe devolver 404 cuando la carga no existe."""
        mock_update_status.side_effect = HTTPException(status_code=404, detail="Carga no encontrada")
        
        response = client.patch("/load/999/status", json={"active": True})
        
        assert response.status_code == 404
        mock_update_status.assert_called_once_with(999, True)

    def test_update_load_status_missing_body(self, client):
        """Debe devolver error de validación cuando falta el body."""
        response = client.patch("/load/1/status")
        
        assert response.status_code == 422  # Validation error

    def test_update_load_status_missing_active_field(self, client):
        """Debe devolver error de validación cuando falta el campo active."""
        response = client.patch("/load/1/status", json={})
        
        assert response.status_code == 422  # Validation error

    def test_update_load_status_invalid_active_type(self, client):
        """Debe devolver error de validación cuando active no es un tipo válido."""
        # Nota: Pydantic coerce strings como "yes"/"true" a True, por lo que usamos un objeto
        response = client.patch("/load/1/status", json={"active": {"invalid": "type"}})
        
        assert response.status_code == 422  # Validation error

    def test_update_load_status_invalid_id_type(self, client):
        """Debe devolver error de validación para ID no numérico."""
        response = client.patch("/load/abc/status", json={"active": True})
        
        assert response.status_code == 422  # Validation error

    @patch("routes.load.update_load_status")
    def test_update_load_status_service_error(self, mock_update_status, client):
        """Debe propagar el error HTTP cuando el servicio falla."""
        mock_update_status.side_effect = HTTPException(status_code=500, detail="Error interno")
        
        response = client.patch("/load/1/status", json={"active": True})
        
        assert response.status_code == 500
        mock_update_status.assert_called_once_with(1, True)

    @patch("routes.load.update_load_status")
    def test_update_load_status_returns_updated_load(self, mock_update_status, client, sample_load_dict):
        """Debe devolver la carga actualizada con todos sus campos."""
        mock_update_status.return_value = sample_load_dict
        
        response = client.patch("/load/1/status", json={"active": True})
        
        assert response.status_code == 200
        data = response.json()
        assert "id" in data
        assert "name" in data
        assert "active" in data
        assert "p" in data
        assert "q" in data
        mock_update_status.assert_called_once_with(1, True)

