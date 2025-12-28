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
def sample_bus_dict():
    """Diccionario que simula un bus devuelto por el servicio."""
    return {
        "id": 1,
        "grid_id": 1,
        "idtag": "bus-001",
        "name": "Bus 1",
        "code": "B001",
        "vnom": 110.0,
        "vm0": 1.0,
        "va0": 0.0,
        "vmin": 0.95,
        "vmax": 1.05,
        "vm_cost": 100.0,
        "angle_min": -30.0,
        "angle_max": 30.0,
        "angle_cost": 10.0,
        "r_fault": 0.01,
        "x_fault": 0.1,
        "x": 100.0,
        "y": 200.0,
        "longitude": -3.7,
        "latitude": 40.4,
        "is_slack": True,
        "active": True,
        "is_dc": False,
        "graphic_type": "rectangle",
        "h": 20.0,
        "w": 40.0,
        "country": "ES",
        "area": "Area1",
        "zone": "Zone1",
        "substation": "Sub1",
        "voltage_level": "110kV",
        "bus_bar": "BB1",
        "ph_a": True,
        "ph_b": True,
        "ph_c": True,
        "ph_n": False,
        "is_grounded": False,
        "active_prof": None,
        "vmin_prof": None,
        "vmax_prof": None,
    }


@pytest.fixture
def sample_buses_list(sample_bus_dict):
    """Lista de buses para probar list_buses."""
    bus2 = sample_bus_dict.copy()
    bus2["id"] = 2
    bus2["idtag"] = "bus-002"
    bus2["name"] = "Bus 2"
    return [sample_bus_dict, bus2]


# =====================================================================
# Tests para GET /bus/ (list_buses)
# =====================================================================

class TestListBuses:
    """Tests para el endpoint GET /bus/"""

    @patch("routes.bus.bus.list_buses")
    def test_list_buses_returns_list(self, mock_list_buses, client, sample_buses_list):
        """Debe devolver una lista de buses cuando hay datos."""
        mock_list_buses.return_value = sample_buses_list
        
        response = client.get("/bus/")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 2
        assert data[0]["id"] == 1
        assert data[1]["id"] == 2
        mock_list_buses.assert_called_once()

    @patch("routes.bus.bus.list_buses")
    def test_list_buses_returns_empty_list(self, mock_list_buses, client):
        """Debe devolver una lista vacía cuando no hay buses."""
        mock_list_buses.return_value = []
        
        response = client.get("/bus/")
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 0
        mock_list_buses.assert_called_once()

    @patch("routes.bus.bus.list_buses")
    def test_list_buses_service_error(self, mock_list_buses, client):
        """Debe propagar el error HTTP cuando el servicio falla."""
        mock_list_buses.side_effect = HTTPException(status_code=500, detail="Error interno del servidor")
        
        response = client.get("/bus/")
        
        assert response.status_code == 500
        mock_list_buses.assert_called_once()


# =====================================================================
# Tests para GET /bus/{bus_id} (get_bus)
# =====================================================================

class TestGetBus:
    """Tests para el endpoint GET /bus/{bus_id}"""

    @patch("routes.bus.bus.get_bus")
    def test_get_bus_success(self, mock_get_bus, client, sample_bus_dict):
        """Debe devolver un bus cuando existe."""
        mock_get_bus.return_value = sample_bus_dict
        
        response = client.get("/bus/1")
        
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == 1
        assert data["name"] == "Bus 1"
        assert data["idtag"] == "bus-001"
        mock_get_bus.assert_called_once_with(1)

    @patch("routes.bus.bus.get_bus")
    def test_get_bus_not_found(self, mock_get_bus, client):
        """Debe devolver 404 cuando el bus no existe."""
        mock_get_bus.side_effect = HTTPException(status_code=404, detail="Bus no encontrado")
        
        response = client.get("/bus/999")
        
        assert response.status_code == 404
        mock_get_bus.assert_called_once_with(999)

    @patch("routes.bus.bus.get_bus")
    def test_get_bus_different_ids(self, mock_get_bus, client, sample_bus_dict):
        """Debe pasar el ID correcto al servicio."""
        sample_bus_dict["id"] = 42
        mock_get_bus.return_value = sample_bus_dict
        
        response = client.get("/bus/42")
        
        assert response.status_code == 200
        assert response.json()["id"] == 42
        mock_get_bus.assert_called_once_with(42)

    def test_get_bus_invalid_id_type(self, client):
        """Debe devolver error de validación para ID no numérico."""
        response = client.get("/bus/abc")
        
        assert response.status_code == 422  # Validation error

    @patch("routes.bus.bus.get_bus")
    def test_get_bus_service_error(self, mock_get_bus, client):
        """Debe propagar el error HTTP cuando el servicio falla."""
        mock_get_bus.side_effect = HTTPException(status_code=500, detail="Error interno")
        
        response = client.get("/bus/1")
        
        assert response.status_code == 500
        mock_get_bus.assert_called_once_with(1)


# =====================================================================
# Tests para PATCH /bus/{bus_id}/status (update_bus_status)
# =====================================================================

class TestUpdateBusStatus:
    """Tests para el endpoint PATCH /bus/{bus_id}/status"""

    @patch("routes.bus.bus.update_bus_status")
    def test_update_bus_status_activate(self, mock_update_status, client, sample_bus_dict):
        """Debe activar un bus correctamente."""
        sample_bus_dict["active"] = True
        mock_update_status.return_value = sample_bus_dict
        
        response = client.patch("/bus/1/status", json={"active": True})
        
        assert response.status_code == 200
        assert response.json()["active"] is True
        mock_update_status.assert_called_once_with(1, True)

    @patch("routes.bus.bus.update_bus_status")
    def test_update_bus_status_deactivate(self, mock_update_status, client, sample_bus_dict):
        """Debe desactivar un bus correctamente."""
        sample_bus_dict["active"] = False
        mock_update_status.return_value = sample_bus_dict
        
        response = client.patch("/bus/1/status", json={"active": False})
        
        assert response.status_code == 200
        assert response.json()["active"] is False
        mock_update_status.assert_called_once_with(1, False)

    @patch("routes.bus.bus.update_bus_status")
    def test_update_bus_status_not_found(self, mock_update_status, client):
        """Debe devolver 404 cuando el bus no existe."""
        mock_update_status.side_effect = HTTPException(status_code=404, detail="Bus no encontrado")
        
        response = client.patch("/bus/999/status", json={"active": True})
        
        assert response.status_code == 404
        mock_update_status.assert_called_once_with(999, True)

    def test_update_bus_status_missing_body(self, client):
        """Debe devolver error de validación cuando falta el body."""
        response = client.patch("/bus/1/status")
        
        assert response.status_code == 422  # Validation error

    def test_update_bus_status_missing_active_field(self, client):
        """Debe devolver error de validación cuando falta el campo active."""
        response = client.patch("/bus/1/status", json={})
        
        assert response.status_code == 422  # Validation error

    def test_update_bus_status_invalid_active_type(self, client):
        """Debe devolver error de validación cuando active no es un tipo válido."""
        # Nota: Pydantic coerce strings como "yes"/"true" a True, por lo que usamos un objeto
        response = client.patch("/bus/1/status", json={"active": {"invalid": "type"}})
        
        assert response.status_code == 422  # Validation error

    def test_update_bus_status_invalid_id_type(self, client):
        """Debe devolver error de validación para ID no numérico."""
        response = client.patch("/bus/abc/status", json={"active": True})
        
        assert response.status_code == 422  # Validation error

    @patch("routes.bus.bus.update_bus_status")
    def test_update_bus_status_service_error(self, mock_update_status, client):
        """Debe propagar el error HTTP cuando el servicio falla."""
        mock_update_status.side_effect = HTTPException(status_code=500, detail="Error interno")
        
        response = client.patch("/bus/1/status", json={"active": True})
        
        assert response.status_code == 500
        mock_update_status.assert_called_once_with(1, True)

    @patch("routes.bus.bus.update_bus_status")
    def test_update_bus_status_returns_updated_bus(self, mock_update_status, client, sample_bus_dict):
        """Debe devolver el bus actualizado con todos sus campos."""
        mock_update_status.return_value = sample_bus_dict
        
        response = client.patch("/bus/1/status", json={"active": True})
        
        assert response.status_code == 200
        data = response.json()
        assert "id" in data
        assert "name" in data
        assert "active" in data
        assert "vnom" in data
        mock_update_status.assert_called_once_with(1, True)
