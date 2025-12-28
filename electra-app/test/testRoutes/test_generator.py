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
def sample_generator_dict():
    """Diccionario que simula un generador devuelto por el servicio."""
    return {
        "id": 1,
        "grid_id": 1,
        "idtag": "gen-001",
        "name": "Generator 1",
        "code": "G001",
        "bus_id": 1,
        "active": True,
        "p": 100.0,
        "q": 50.0,
        "pf": 0.95,
        "vset": 1.0,
        "snom": 150.0,
        "pmin": 10.0,
        "pmax": 120.0,
        "qmin": -50.0,
        "qmax": 80.0,
        "cost0": 0.0,
        "cost1": 20.0,
        "cost2": 0.05,
        "startup_cost": 500.0,
        "mttf": 8760.0,
        "mttr": 24.0,
    }


@pytest.fixture
def sample_generators_list(sample_generator_dict):
    """Lista de generadores para probar list_generators."""
    gen2 = sample_generator_dict.copy()
    gen2["id"] = 2
    gen2["idtag"] = "gen-002"
    gen2["name"] = "Generator 2"
    return [sample_generator_dict, gen2]


# =====================================================================
# Tests para GET /generator/ (list_generators)
# =====================================================================

class TestListGenerators:
    """Tests para el endpoint GET /generator/"""

    @patch("routes.generator.list_generators")
    def test_list_generators_returns_list(self, mock_list_generators, client, sample_generators_list):
        """Debe devolver una lista de generadores cuando hay datos."""
        mock_list_generators.return_value = sample_generators_list
        
        response = client.get("/generator/")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 2
        assert data[0]["id"] == 1
        assert data[1]["id"] == 2
        mock_list_generators.assert_called_once()

    @patch("routes.generator.list_generators")
    def test_list_generators_returns_empty_list(self, mock_list_generators, client):
        """Debe devolver una lista vacía cuando no hay generadores."""
        mock_list_generators.return_value = []
        
        response = client.get("/generator/")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 0
        mock_list_generators.assert_called_once()

    @patch("routes.generator.list_generators")
    def test_list_generators_service_error(self, mock_list_generators, client):
        """Debe propagar el error HTTP cuando el servicio falla."""
        mock_list_generators.side_effect = HTTPException(status_code=500, detail="Error interno del servidor")
        
        response = client.get("/generator/")
        
        assert response.status_code == 500
        mock_list_generators.assert_called_once()


# =====================================================================
# Tests para GET /generator/{generator_id} (get_generator)
# =====================================================================

class TestGetGenerator:
    """Tests para el endpoint GET /generator/{generator_id}"""

    @patch("routes.generator.get_generator")
    def test_get_generator_success(self, mock_get_generator, client, sample_generator_dict):
        """Debe devolver un generador cuando existe."""
        mock_get_generator.return_value = sample_generator_dict
        
        response = client.get("/generator/1")
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == 1
        assert data["name"] == "Generator 1"
        assert data["idtag"] == "gen-001"
        mock_get_generator.assert_called_once_with(1)

    @patch("routes.generator.get_generator")
    def test_get_generator_not_found(self, mock_get_generator, client):
        """Debe devolver 404 cuando el generador no existe."""
        mock_get_generator.side_effect = HTTPException(status_code=404, detail="Generador no encontrado")
        
        response = client.get("/generator/999")
        
        assert response.status_code == 404
        mock_get_generator.assert_called_once_with(999)

    @patch("routes.generator.get_generator")
    def test_get_generator_different_ids(self, mock_get_generator, client, sample_generator_dict):
        """Debe pasar el ID correcto al servicio."""
        sample_generator_dict["id"] = 42
        mock_get_generator.return_value = sample_generator_dict
        
        response = client.get("/generator/42")
        
        assert response.status_code == 200
        assert response.json()["id"] == 42
        mock_get_generator.assert_called_once_with(42)

    def test_get_generator_invalid_id_type(self, client):
        """Debe devolver error de validación para ID no numérico."""
        response = client.get("/generator/abc")
        
        assert response.status_code == 422  # Validation error

    @patch("routes.generator.get_generator")
    def test_get_generator_service_error(self, mock_get_generator, client):
        """Debe propagar el error HTTP cuando el servicio falla."""
        mock_get_generator.side_effect = HTTPException(status_code=500, detail="Error interno")
        
        response = client.get("/generator/1")
        
        assert response.status_code == 500
        mock_get_generator.assert_called_once_with(1)


# =====================================================================
# Tests para PATCH /generator/{generator_id}/status (update_generator_status)
# =====================================================================

class TestUpdateGeneratorStatus:
    """Tests para el endpoint PATCH /generator/{generator_id}/status"""

    @patch("routes.generator.update_generator_status")
    def test_update_generator_status_activate(self, mock_update_status, client, sample_generator_dict):
        """Debe activar un generador correctamente."""
        sample_generator_dict["active"] = True
        mock_update_status.return_value = sample_generator_dict
        
        response = client.patch("/generator/1/status", json={"active": True})
        
        assert response.status_code == 200
        assert response.json()["active"] is True
        mock_update_status.assert_called_once_with(1, True)

    @patch("routes.generator.update_generator_status")
    def test_update_generator_status_deactivate(self, mock_update_status, client, sample_generator_dict):
        """Debe desactivar un generador correctamente."""
        sample_generator_dict["active"] = False
        mock_update_status.return_value = sample_generator_dict
        
        response = client.patch("/generator/1/status", json={"active": False})
        
        assert response.status_code == 200
        assert response.json()["active"] is False
        mock_update_status.assert_called_once_with(1, False)

    @patch("routes.generator.update_generator_status")
    def test_update_generator_status_not_found(self, mock_update_status, client):
        """Debe devolver 404 cuando el generador no existe."""
        mock_update_status.side_effect = HTTPException(status_code=404, detail="Generador no encontrado")
        
        response = client.patch("/generator/999/status", json={"active": True})
        
        assert response.status_code == 404
        mock_update_status.assert_called_once_with(999, True)

    def test_update_generator_status_missing_body(self, client):
        """Debe devolver error de validación cuando falta el body."""
        response = client.patch("/generator/1/status")
        
        assert response.status_code == 422  # Validation error

    def test_update_generator_status_missing_active_field(self, client):
        """Debe devolver error de validación cuando falta el campo active."""
        response = client.patch("/generator/1/status", json={})
        
        assert response.status_code == 422  # Validation error

    def test_update_generator_status_invalid_active_type(self, client):
        """Debe devolver error de validación cuando active no es un tipo válido."""
        # Nota: Pydantic coerce strings como "yes"/"true" a True, por lo que usamos un objeto
        response = client.patch("/generator/1/status", json={"active": {"invalid": "type"}})
        
        assert response.status_code == 422  # Validation error

    def test_update_generator_status_invalid_id_type(self, client):
        """Debe devolver error de validación para ID no numérico."""
        response = client.patch("/generator/abc/status", json={"active": True})
        
        assert response.status_code == 422  # Validation error

    @patch("routes.generator.update_generator_status")
    def test_update_generator_status_service_error(self, mock_update_status, client):
        """Debe propagar el error HTTP cuando el servicio falla."""
        mock_update_status.side_effect = HTTPException(status_code=500, detail="Error interno")
        
        response = client.patch("/generator/1/status", json={"active": True})
        
        assert response.status_code == 500
        mock_update_status.assert_called_once_with(1, True)

    @patch("routes.generator.update_generator_status")
    def test_update_generator_status_returns_updated_generator(self, mock_update_status, client, sample_generator_dict):
        """Debe devolver el generador actualizado con todos sus campos."""
        mock_update_status.return_value = sample_generator_dict
        
        response = client.patch("/generator/1/status", json={"active": True})
        
        assert response.status_code == 200
        data = response.json()
        assert "id" in data
        assert "name" in data
        assert "active" in data
        assert "p" in data
        mock_update_status.assert_called_once_with(1, True)

