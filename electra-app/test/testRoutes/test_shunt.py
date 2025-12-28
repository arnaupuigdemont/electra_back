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
def sample_shunt_dict():
    """Diccionario que simula un shunt devuelto por el servicio."""
    return {
        "id": 1,
        "grid_id": 1,
        "idtag": "shunt-001",
        "name": "Shunt 1",
        "code": "SH001",
        "bus_id": 1,
        "active": True,
        "g": 0.0,
        "b": 0.05,
        "mttf": 8760.0,
        "mttr": 24.0,
    }


@pytest.fixture
def sample_shunts_list(sample_shunt_dict):
    """Lista de shunts para probar list_shunts."""
    shunt2 = sample_shunt_dict.copy()
    shunt2["id"] = 2
    shunt2["idtag"] = "shunt-002"
    shunt2["name"] = "Shunt 2"
    return [sample_shunt_dict, shunt2]


# =====================================================================
# Tests para GET /shunt/ (list_shunts)
# =====================================================================

class TestListShunts:
    """Tests para el endpoint GET /shunt/"""

    @patch("routes.shunt.list_shunts")
    def test_list_shunts_returns_list(self, mock_list_shunts, client, sample_shunts_list):
        """Debe devolver una lista de shunts cuando hay datos."""
        mock_list_shunts.return_value = sample_shunts_list
        
        response = client.get("/shunt/")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 2
        assert data[0]["id"] == 1
        assert data[1]["id"] == 2
        mock_list_shunts.assert_called_once()

    @patch("routes.shunt.list_shunts")
    def test_list_shunts_returns_empty_list(self, mock_list_shunts, client):
        """Debe devolver una lista vacía cuando no hay shunts."""
        mock_list_shunts.return_value = []
        
        response = client.get("/shunt/")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 0
        mock_list_shunts.assert_called_once()

    @patch("routes.shunt.list_shunts")
    def test_list_shunts_service_error(self, mock_list_shunts, client):
        """Debe propagar el error HTTP cuando el servicio falla."""
        mock_list_shunts.side_effect = HTTPException(status_code=500, detail="Error interno del servidor")
        
        response = client.get("/shunt/")
        
        assert response.status_code == 500
        mock_list_shunts.assert_called_once()


# =====================================================================
# Tests para GET /shunt/{shunt_id} (get_shunt)
# =====================================================================

class TestGetShunt:
    """Tests para el endpoint GET /shunt/{shunt_id}"""

    @patch("routes.shunt.get_shunt")
    def test_get_shunt_success(self, mock_get_shunt, client, sample_shunt_dict):
        """Debe devolver un shunt cuando existe."""
        mock_get_shunt.return_value = sample_shunt_dict
        
        response = client.get("/shunt/1")
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == 1
        assert data["name"] == "Shunt 1"
        assert data["idtag"] == "shunt-001"
        mock_get_shunt.assert_called_once_with(1)

    @patch("routes.shunt.get_shunt")
    def test_get_shunt_not_found(self, mock_get_shunt, client):
        """Debe devolver 404 cuando el shunt no existe."""
        mock_get_shunt.side_effect = HTTPException(status_code=404, detail="Shunt no encontrado")
        
        response = client.get("/shunt/999")
        
        assert response.status_code == 404
        mock_get_shunt.assert_called_once_with(999)

    @patch("routes.shunt.get_shunt")
    def test_get_shunt_different_ids(self, mock_get_shunt, client, sample_shunt_dict):
        """Debe pasar el ID correcto al servicio."""
        sample_shunt_dict["id"] = 42
        mock_get_shunt.return_value = sample_shunt_dict
        
        response = client.get("/shunt/42")
        
        assert response.status_code == 200
        assert response.json()["id"] == 42
        mock_get_shunt.assert_called_once_with(42)

    def test_get_shunt_invalid_id_type(self, client):
        """Debe devolver error de validación para ID no numérico."""
        response = client.get("/shunt/abc")
        
        assert response.status_code == 422  # Validation error

    @patch("routes.shunt.get_shunt")
    def test_get_shunt_service_error(self, mock_get_shunt, client):
        """Debe propagar el error HTTP cuando el servicio falla."""
        mock_get_shunt.side_effect = HTTPException(status_code=500, detail="Error interno")
        
        response = client.get("/shunt/1")
        
        assert response.status_code == 500
        mock_get_shunt.assert_called_once_with(1)


# =====================================================================
# Tests para PATCH /shunt/{shunt_id}/status (update_shunt_status)
# =====================================================================

class TestUpdateShuntStatus:
    """Tests para el endpoint PATCH /shunt/{shunt_id}/status"""

    @patch("routes.shunt.update_shunt_status")
    def test_update_shunt_status_activate(self, mock_update_status, client, sample_shunt_dict):
        """Debe activar un shunt correctamente."""
        sample_shunt_dict["active"] = True
        mock_update_status.return_value = sample_shunt_dict
        
        response = client.patch("/shunt/1/status", json={"active": True})
        
        assert response.status_code == 200
        assert response.json()["active"] is True
        mock_update_status.assert_called_once_with(1, True)

    @patch("routes.shunt.update_shunt_status")
    def test_update_shunt_status_deactivate(self, mock_update_status, client, sample_shunt_dict):
        """Debe desactivar un shunt correctamente."""
        sample_shunt_dict["active"] = False
        mock_update_status.return_value = sample_shunt_dict
        
        response = client.patch("/shunt/1/status", json={"active": False})
        
        assert response.status_code == 200
        assert response.json()["active"] is False
        mock_update_status.assert_called_once_with(1, False)

    @patch("routes.shunt.update_shunt_status")
    def test_update_shunt_status_not_found(self, mock_update_status, client):
        """Debe devolver 404 cuando el shunt no existe."""
        mock_update_status.side_effect = HTTPException(status_code=404, detail="Shunt no encontrado")
        
        response = client.patch("/shunt/999/status", json={"active": True})
        
        assert response.status_code == 404
        mock_update_status.assert_called_once_with(999, True)

    def test_update_shunt_status_missing_body(self, client):
        """Debe devolver error de validación cuando falta el body."""
        response = client.patch("/shunt/1/status")
        
        assert response.status_code == 422  # Validation error

    def test_update_shunt_status_missing_active_field(self, client):
        """Debe devolver error de validación cuando falta el campo active."""
        response = client.patch("/shunt/1/status", json={})
        
        assert response.status_code == 422  # Validation error

    def test_update_shunt_status_invalid_active_type(self, client):
        """Debe devolver error de validación cuando active no es un tipo válido."""
        # Nota: Pydantic coerce strings como "yes"/"true" a True, por lo que usamos un objeto
        response = client.patch("/shunt/1/status", json={"active": {"invalid": "type"}})
        
        assert response.status_code == 422  # Validation error

    def test_update_shunt_status_invalid_id_type(self, client):
        """Debe devolver error de validación para ID no numérico."""
        response = client.patch("/shunt/abc/status", json={"active": True})
        
        assert response.status_code == 422  # Validation error

    @patch("routes.shunt.update_shunt_status")
    def test_update_shunt_status_service_error(self, mock_update_status, client):
        """Debe propagar el error HTTP cuando el servicio falla."""
        mock_update_status.side_effect = HTTPException(status_code=500, detail="Error interno")
        
        response = client.patch("/shunt/1/status", json={"active": True})
        
        assert response.status_code == 500
        mock_update_status.assert_called_once_with(1, True)

    @patch("routes.shunt.update_shunt_status")
    def test_update_shunt_status_returns_updated_shunt(self, mock_update_status, client, sample_shunt_dict):
        """Debe devolver el shunt actualizado con todos sus campos."""
        mock_update_status.return_value = sample_shunt_dict
        
        response = client.patch("/shunt/1/status", json={"active": True})
        
        assert response.status_code == 200
        data = response.json()
        assert "id" in data
        assert "name" in data
        assert "active" in data
        assert "g" in data
        assert "b" in data
        mock_update_status.assert_called_once_with(1, True)
