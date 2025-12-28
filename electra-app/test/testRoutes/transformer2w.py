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
def sample_transformer_dict():
    """Diccionario que simula un transformador devuelto por el servicio."""
    return {
        "id": 1,
        "grid_id": 1,
        "idtag": "trafo-001",
        "name": "Transformer 1",
        "code": "T001",
        "bus_from_id": 1,
        "bus_to_id": 2,
        "active": True,
        "rate": 100.0,
        "r": 0.01,
        "x": 0.1,
        "g": 0.0,
        "b": 0.0,
        "tap": 1.0,
        "shift_angle": 0.0,
        "mttf": 8760.0,
        "mttr": 24.0,
    }


@pytest.fixture
def sample_transformers_list(sample_transformer_dict):
    """Lista de transformadores para probar list_transformers2w."""
    trafo2 = sample_transformer_dict.copy()
    trafo2["id"] = 2
    trafo2["idtag"] = "trafo-002"
    trafo2["name"] = "Transformer 2"
    return [sample_transformer_dict, trafo2]


# =====================================================================
# Tests para GET /transformer2w/ (list_transformers2w)
# =====================================================================

class TestListTransformers2w:
    """Tests para el endpoint GET /transformer2w/"""

    @patch("routes.transformer2w.list_transformers2w")
    def test_list_transformers_returns_list(self, mock_list_transformers, client, sample_transformers_list):
        """Debe devolver una lista de transformadores cuando hay datos."""
        mock_list_transformers.return_value = sample_transformers_list
        
        response = client.get("/transformer2w/")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 2
        assert data[0]["id"] == 1
        assert data[1]["id"] == 2
        mock_list_transformers.assert_called_once()

    @patch("routes.transformer2w.list_transformers2w")
    def test_list_transformers_returns_empty_list(self, mock_list_transformers, client):
        """Debe devolver una lista vacía cuando no hay transformadores."""
        mock_list_transformers.return_value = []
        
        response = client.get("/transformer2w/")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 0
        mock_list_transformers.assert_called_once()

    @patch("routes.transformer2w.list_transformers2w")
    def test_list_transformers_service_error(self, mock_list_transformers, client):
        """Debe propagar el error HTTP cuando el servicio falla."""
        mock_list_transformers.side_effect = HTTPException(status_code=500, detail="Error interno del servidor")
        
        response = client.get("/transformer2w/")
        
        assert response.status_code == 500
        mock_list_transformers.assert_called_once()


# =====================================================================
# Tests para GET /transformer2w/{transformer_id} (get_transformer2w)
# =====================================================================

class TestGetTransformer2w:
    """Tests para el endpoint GET /transformer2w/{transformer_id}"""

    @patch("routes.transformer2w.get_transformer2w")
    def test_get_transformer_success(self, mock_get_transformer, client, sample_transformer_dict):
        """Debe devolver un transformador cuando existe."""
        mock_get_transformer.return_value = sample_transformer_dict
        
        response = client.get("/transformer2w/1")
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == 1
        assert data["name"] == "Transformer 1"
        assert data["idtag"] == "trafo-001"
        mock_get_transformer.assert_called_once_with(1)

    @patch("routes.transformer2w.get_transformer2w")
    def test_get_transformer_not_found(self, mock_get_transformer, client):
        """Debe devolver 404 cuando el transformador no existe."""
        mock_get_transformer.side_effect = HTTPException(status_code=404, detail="Transformador no encontrado")
        
        response = client.get("/transformer2w/999")
        
        assert response.status_code == 404
        mock_get_transformer.assert_called_once_with(999)

    @patch("routes.transformer2w.get_transformer2w")
    def test_get_transformer_different_ids(self, mock_get_transformer, client, sample_transformer_dict):
        """Debe pasar el ID correcto al servicio."""
        sample_transformer_dict["id"] = 42
        mock_get_transformer.return_value = sample_transformer_dict
        
        response = client.get("/transformer2w/42")
        
        assert response.status_code == 200
        assert response.json()["id"] == 42
        mock_get_transformer.assert_called_once_with(42)

    def test_get_transformer_invalid_id_type(self, client):
        """Debe devolver error de validación para ID no numérico."""
        response = client.get("/transformer2w/abc")
        
        assert response.status_code == 422  # Validation error

    @patch("routes.transformer2w.get_transformer2w")
    def test_get_transformer_service_error(self, mock_get_transformer, client):
        """Debe propagar el error HTTP cuando el servicio falla."""
        mock_get_transformer.side_effect = HTTPException(status_code=500, detail="Error interno")
        
        response = client.get("/transformer2w/1")
        
        assert response.status_code == 500
        mock_get_transformer.assert_called_once_with(1)


# =====================================================================
# Tests para PATCH /transformer2w/{transformer_id}/status (update_transformer_status)
# =====================================================================

class TestUpdateTransformerStatus:
    """Tests para el endpoint PATCH /transformer2w/{transformer_id}/status"""

    @patch("routes.transformer2w.update_transformer_status")
    def test_update_transformer_status_activate(self, mock_update_status, client, sample_transformer_dict):
        """Debe activar un transformador correctamente."""
        sample_transformer_dict["active"] = True
        mock_update_status.return_value = sample_transformer_dict
        
        response = client.patch("/transformer2w/1/status", json={"active": True})
        
        assert response.status_code == 200
        assert response.json()["active"] is True
        mock_update_status.assert_called_once_with(1, True)

    @patch("routes.transformer2w.update_transformer_status")
    def test_update_transformer_status_deactivate(self, mock_update_status, client, sample_transformer_dict):
        """Debe desactivar un transformador correctamente."""
        sample_transformer_dict["active"] = False
        mock_update_status.return_value = sample_transformer_dict
        
        response = client.patch("/transformer2w/1/status", json={"active": False})
        
        assert response.status_code == 200
        assert response.json()["active"] is False
        mock_update_status.assert_called_once_with(1, False)

    @patch("routes.transformer2w.update_transformer_status")
    def test_update_transformer_status_not_found(self, mock_update_status, client):
        """Debe devolver 404 cuando el transformador no existe."""
        mock_update_status.side_effect = HTTPException(status_code=404, detail="Transformador no encontrado")
        
        response = client.patch("/transformer2w/999/status", json={"active": True})
        
        assert response.status_code == 404
        mock_update_status.assert_called_once_with(999, True)

    def test_update_transformer_status_missing_body(self, client):
        """Debe devolver error de validación cuando falta el body."""
        response = client.patch("/transformer2w/1/status")
        
        assert response.status_code == 422  # Validation error

    def test_update_transformer_status_missing_active_field(self, client):
        """Debe devolver error de validación cuando falta el campo active."""
        response = client.patch("/transformer2w/1/status", json={})
        
        assert response.status_code == 422  # Validation error

    def test_update_transformer_status_invalid_active_type(self, client):
        """Debe devolver error de validación cuando active no es un tipo válido."""
        # Nota: Pydantic coerce strings como "yes"/"true" a True, por lo que usamos un objeto
        response = client.patch("/transformer2w/1/status", json={"active": {"invalid": "type"}})
        
        assert response.status_code == 422  # Validation error

    def test_update_transformer_status_invalid_id_type(self, client):
        """Debe devolver error de validación para ID no numérico."""
        response = client.patch("/transformer2w/abc/status", json={"active": True})
        
        assert response.status_code == 422  # Validation error

    @patch("routes.transformer2w.update_transformer_status")
    def test_update_transformer_status_service_error(self, mock_update_status, client):
        """Debe propagar el error HTTP cuando el servicio falla."""
        mock_update_status.side_effect = HTTPException(status_code=500, detail="Error interno")
        
        response = client.patch("/transformer2w/1/status", json={"active": True})
        
        assert response.status_code == 500
        mock_update_status.assert_called_once_with(1, True)

    @patch("routes.transformer2w.update_transformer_status")
    def test_update_transformer_status_returns_updated_transformer(self, mock_update_status, client, sample_transformer_dict):
        """Debe devolver el transformador actualizado con todos sus campos."""
        mock_update_status.return_value = sample_transformer_dict
        
        response = client.patch("/transformer2w/1/status", json={"active": True})
        
        assert response.status_code == 200
        data = response.json()
        assert "id" in data
        assert "name" in data
        assert "active" in data
        assert "r" in data
        assert "x" in data
        assert "tap" in data
        mock_update_status.assert_called_once_with(1, True)
