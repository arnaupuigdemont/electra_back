import sys
import os

# Añadir el directorio raíz de la aplicación al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import pytest
from unittest.mock import patch, MagicMock
from fastapi import HTTPException

from services.bus import list_buses, get_bus, update_bus_status


# =====================================================================
# Fixtures y datos de prueba
# =====================================================================

@pytest.fixture
def sample_bus_tuple():
    """Tuple que simula una fila de bus devuelta por el repositorio."""
    return (
        1,              # id
        1,              # grid_id
        "bus-001",      # idtag
        "Bus 1",        # name
        "B001",         # code
        110.0,          # vnom
        1.0,            # vm0
        0.0,            # va0
        0.95,           # vmin
        1.05,           # vmax
        100.0,          # vm_cost
        -30.0,          # angle_min
        30.0,           # angle_max
        10.0,           # angle_cost
        0.01,           # r_fault
        0.1,            # x_fault
        100.0,          # x
        200.0,          # y
        -3.7,           # longitude
        40.4,           # latitude
        True,           # is_slack
        True,           # active
        False,          # is_dc
        "rectangle",    # graphic_type
        20.0,           # h
        40.0,           # w
        "ES",           # country
        "Area1",        # area
        "Zone1",        # zone
        "Sub1",         # substation
        "110kV",        # voltage_level
        "BB1",          # bus_bar
        True,           # ph_a
        True,           # ph_b
        True,           # ph_c
        False,          # ph_n
        False,          # is_grounded
        None,           # active_prof
        None,           # vmin_prof
        None,           # vmax_prof
    )


@pytest.fixture
def sample_bus_dict():
    """Diccionario que simula una fila de bus como dict."""
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


# =====================================================================
# Tests para list_buses()
# =====================================================================

class TestListBuses:
    """Tests para la función list_buses."""

    @patch("services.bus.repo_list_buses")
    def test_list_buses_returns_empty_list_when_no_buses(self, mock_repo_list):
        """Debe devolver lista vacía cuando no hay buses."""
        mock_repo_list.return_value = None
        
        result = list_buses()
        
        assert result == []
        mock_repo_list.assert_called_once()

    @patch("services.bus.repo_list_buses")
    def test_list_buses_returns_empty_list_when_empty_rows(self, mock_repo_list):
        """Debe devolver lista vacía cuando rows está vacío."""
        mock_repo_list.return_value = []
        
        result = list_buses()
        
        assert result == []
        mock_repo_list.assert_called_once()

    @patch("services.bus.repo_list_buses")
    def test_list_buses_with_tuple_rows(self, mock_repo_list, sample_bus_tuple):
        """Debe normalizar correctamente filas en formato tuple."""
        mock_repo_list.return_value = [sample_bus_tuple]
        
        result = list_buses()
        
        assert len(result) == 1
        assert result[0]["id"] == 1
        assert result[0]["grid_id"] == 1
        assert result[0]["idtag"] == "bus-001"
        assert result[0]["name"] == "Bus 1"
        assert result[0]["vnom"] == 110.0
        assert result[0]["is_slack"] is True
        assert result[0]["active"] is True
        mock_repo_list.assert_called_once()

    @patch("services.bus.repo_list_buses")
    def test_list_buses_with_dict_rows(self, mock_repo_list, sample_bus_dict):
        """Debe manejar correctamente filas en formato dict."""
        # Crear un objeto que actúe como dict (simulando RealDictRow)
        mock_row = MagicMock()
        mock_row.__iter__ = MagicMock(return_value=iter(sample_bus_dict.items()))
        mock_row.keys = MagicMock(return_value=sample_bus_dict.keys())
        mock_row.values = MagicMock(return_value=sample_bus_dict.values())
        mock_row.items = MagicMock(return_value=sample_bus_dict.items())
        
        # Simular que dict(row) funciona
        def mock_dict_conversion():
            return sample_bus_dict
        
        mock_repo_list.return_value = [sample_bus_dict]
        
        result = list_buses()
        
        assert len(result) == 1
        assert result[0]["id"] == 1
        assert result[0]["idtag"] == "bus-001"

    @patch("services.bus.repo_list_buses")
    def test_list_buses_with_multiple_buses(self, mock_repo_list, sample_bus_tuple):
        """Debe manejar múltiples buses correctamente."""
        bus2_tuple = (
            2, 1, "bus-002", "Bus 2", "B002", 220.0, 1.0, 0.0, 0.95, 1.05,
            100.0, -30.0, 30.0, 10.0, 0.01, 0.1, 150.0, 250.0, -3.8, 40.5,
            False, True, False, "rectangle", 20.0, 40.0, "ES", "Area1",
            "Zone1", "Sub2", "220kV", "BB2", True, True, True, False, False,
            None, None, None
        )
        mock_repo_list.return_value = [sample_bus_tuple, bus2_tuple]
        
        result = list_buses()
        
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[1]["id"] == 2
        assert result[0]["idtag"] == "bus-001"
        assert result[1]["idtag"] == "bus-002"


# =====================================================================
# Tests para get_bus()
# =====================================================================

class TestGetBus:
    """Tests para la función get_bus."""

    @patch("services.bus.repo_get_bus_by_id")
    def test_get_bus_not_found_raises_404(self, mock_repo_get):
        """Debe lanzar HTTPException 404 cuando el bus no existe."""
        mock_repo_get.return_value = None
        
        with pytest.raises(HTTPException) as exc_info:
            get_bus(999)
        
        assert exc_info.value.status_code == 404
        assert exc_info.value.detail == "Bus not found"
        mock_repo_get.assert_called_once_with(999)

    @patch("services.bus.repo_get_bus_by_id")
    def test_get_bus_with_tuple_row(self, mock_repo_get, sample_bus_tuple):
        """Debe devolver bus correctamente cuando la fila es tuple."""
        mock_repo_get.return_value = sample_bus_tuple
        
        result = get_bus(1)
        
        assert result["id"] == 1
        assert result["grid_id"] == 1
        assert result["idtag"] == "bus-001"
        assert result["name"] == "Bus 1"
        assert result["vnom"] == 110.0
        assert result["is_slack"] is True
        assert result["active"] is True
        mock_repo_get.assert_called_once_with(1)

    @patch("services.bus.repo_get_bus_by_id")
    def test_get_bus_with_dict_row(self, mock_repo_get, sample_bus_dict):
        """Debe devolver bus correctamente cuando la fila es dict."""
        mock_repo_get.return_value = sample_bus_dict
        
        result = get_bus(1)
        
        assert result["id"] == 1
        assert result["idtag"] == "bus-001"
        mock_repo_get.assert_called_once_with(1)

    @patch("services.bus.repo_get_bus_by_id")
    def test_get_bus_returns_all_fields(self, mock_repo_get, sample_bus_tuple):
        """Debe devolver todos los campos del bus."""
        mock_repo_get.return_value = sample_bus_tuple
        
        result = get_bus(1)
        
        expected_fields = [
            "id", "grid_id", "idtag", "name", "code", "vnom", "vm0", "va0",
            "vmin", "vmax", "vm_cost", "angle_min", "angle_max", "angle_cost",
            "r_fault", "x_fault", "x", "y", "longitude", "latitude", "is_slack",
            "active", "is_dc", "graphic_type", "h", "w", "country", "area",
            "zone", "substation", "voltage_level", "bus_bar", "ph_a", "ph_b",
            "ph_c", "ph_n", "is_grounded", "active_prof", "vmin_prof", "vmax_prof"
        ]
        
        for field in expected_fields:
            assert field in result


# =====================================================================
# Tests para update_bus_status()
# =====================================================================

class TestUpdateBusStatus:
    """Tests para la función update_bus_status."""

    @patch("services.bus.repo_get_bus_by_id")
    def test_update_bus_status_bus_not_found_raises_404(self, mock_repo_get):
        """Debe lanzar HTTPException 404 cuando el bus no existe."""
        mock_repo_get.return_value = None
        
        with pytest.raises(HTTPException) as exc_info:
            update_bus_status(999, False)
        
        assert exc_info.value.status_code == 404
        assert exc_info.value.detail == "Bus not found"

    @patch("services.bus.gce")
    @patch("services.bus.get_tmp_file_path")
    @patch("services.bus.repo_update_elements_by_bus_idtag")
    @patch("services.bus.repo_update_bus_status")
    @patch("services.bus.repo_get_bus_by_id")
    def test_update_bus_status_activate(
        self, mock_repo_get, mock_repo_update, mock_repo_elements,
        mock_get_tmp, mock_gce, sample_bus_tuple
    ):
        """Debe activar un bus correctamente."""
        mock_repo_get.return_value = sample_bus_tuple
        mock_repo_update.return_value = (1, 1, "bus-001")
        mock_get_tmp.return_value = None  # Sin archivo temporal
        
        result = update_bus_status(1, True)
        
        assert result["message"] == "Bus status updated"
        assert result["bus_id"] == 1
        assert result["active"] is True
        mock_repo_update.assert_called_once_with(1, True)
        # No debe llamar a cascade cuando se activa
        mock_repo_elements.assert_not_called()

    @patch("services.bus.gce")
    @patch("services.bus.get_tmp_file_path")
    @patch("services.bus.repo_update_elements_by_bus_idtag")
    @patch("services.bus.repo_update_bus_status")
    @patch("services.bus.repo_get_bus_by_id")
    def test_update_bus_status_deactivate_cascades(
        self, mock_repo_get, mock_repo_update, mock_repo_elements,
        mock_get_tmp, mock_gce, sample_bus_tuple
    ):
        """Debe desactivar un bus y hacer cascade a elementos conectados."""
        mock_repo_get.return_value = sample_bus_tuple
        mock_repo_update.return_value = (1, 1, "bus-001")
        mock_get_tmp.return_value = None  # Sin archivo temporal
        
        result = update_bus_status(1, False)
        
        assert result["message"] == "Bus status updated"
        assert result["bus_id"] == 1
        assert result["active"] is False
        mock_repo_update.assert_called_once_with(1, False)
        # Debe llamar a cascade cuando se desactiva
        mock_repo_elements.assert_called_once_with(1, "bus-001", False)

    @patch("services.bus.repo_update_bus_status")
    @patch("services.bus.repo_get_bus_by_id")
    def test_update_bus_status_update_fails_raises_404(
        self, mock_repo_get, mock_repo_update, sample_bus_tuple
    ):
        """Debe lanzar 404 si la actualización en DB falla."""
        mock_repo_get.return_value = sample_bus_tuple
        mock_repo_update.return_value = None
        
        with pytest.raises(HTTPException) as exc_info:
            update_bus_status(1, False)
        
        assert exc_info.value.status_code == 404
        assert exc_info.value.detail == "Bus not found"

    @patch("services.bus.os.path.exists")
    @patch("services.bus.gce")
    @patch("services.bus.get_tmp_file_path")
    @patch("services.bus.repo_update_elements_by_bus_idtag")
    @patch("services.bus.repo_update_bus_status")
    @patch("services.bus.repo_get_bus_by_id")
    def test_update_bus_status_updates_veragrid_file(
        self, mock_repo_get, mock_repo_update, mock_repo_elements,
        mock_get_tmp, mock_gce, mock_exists, sample_bus_tuple
    ):
        """Debe actualizar el archivo VeraGrid cuando existe."""
        mock_repo_get.return_value = sample_bus_tuple
        mock_repo_update.return_value = (1, 1, "bus-001")
        mock_get_tmp.return_value = "/tmp/test.gridcal"
        mock_exists.return_value = True
        
        # Mock del circuito
        mock_bus = MagicMock()
        mock_bus.idtag = "bus-001"
        mock_bus.active = True
        
        mock_circuit = MagicMock()
        mock_circuit.buses = [mock_bus]
        mock_circuit.generators = []
        mock_circuit.loads = []
        mock_circuit.shunts = []
        
        mock_gce.open_file.return_value = mock_circuit
        
        result = update_bus_status(1, False)
        
        assert result["active"] is False
        mock_gce.open_file.assert_called_once_with("/tmp/test.gridcal")
        mock_gce.save_file.assert_called_once_with(mock_circuit, "/tmp/test.gridcal")
        assert mock_bus.active is False

    @patch("services.bus.os.path.exists")
    @patch("services.bus.gce")
    @patch("services.bus.get_tmp_file_path")
    @patch("services.bus.repo_update_elements_by_bus_idtag")
    @patch("services.bus.repo_update_bus_status")
    @patch("services.bus.repo_get_bus_by_id")
    def test_update_bus_status_deactivate_cascades_in_veragrid(
        self, mock_repo_get, mock_repo_update, mock_repo_elements,
        mock_get_tmp, mock_gce, mock_exists, sample_bus_tuple
    ):
        """Debe desactivar generadores, cargas y shunts en VeraGrid."""
        mock_repo_get.return_value = sample_bus_tuple
        mock_repo_update.return_value = (1, 1, "bus-001")
        mock_get_tmp.return_value = "/tmp/test.gridcal"
        mock_exists.return_value = True
        
        # Mock del bus
        mock_bus = MagicMock()
        mock_bus.idtag = "bus-001"
        mock_bus.active = True
        
        # Mock de generador conectado al bus
        mock_gen = MagicMock()
        mock_gen.bus = mock_bus
        mock_gen.active = True
        
        # Mock de carga conectada al bus
        mock_load = MagicMock()
        mock_load.bus = mock_bus
        mock_load.active = True
        
        # Mock de shunt conectado al bus
        mock_shunt = MagicMock()
        mock_shunt.bus = mock_bus
        mock_shunt.active = True
        
        mock_circuit = MagicMock()
        mock_circuit.buses = [mock_bus]
        mock_circuit.generators = [mock_gen]
        mock_circuit.loads = [mock_load]
        mock_circuit.shunts = [mock_shunt]
        
        mock_gce.open_file.return_value = mock_circuit
        
        result = update_bus_status(1, False)
        
        # Verificar que se desactivaron todos los elementos
        assert mock_bus.active is False
        assert mock_gen.active is False
        assert mock_load.active is False
        assert mock_shunt.active is False

    @patch("services.bus.os.path.exists")
    @patch("services.bus.gce")
    @patch("services.bus.get_tmp_file_path")
    @patch("services.bus.repo_update_elements_by_bus_idtag")
    @patch("services.bus.repo_update_bus_status")
    @patch("services.bus.repo_get_bus_by_id")
    def test_update_bus_status_veragrid_bus_not_found_logs_warning(
        self, mock_repo_get, mock_repo_update, mock_repo_elements,
        mock_get_tmp, mock_gce, mock_exists, sample_bus_tuple
    ):
        """Debe loguear warning cuando el bus no se encuentra en VeraGrid."""
        mock_repo_get.return_value = sample_bus_tuple
        mock_repo_update.return_value = (1, 1, "bus-001")
        mock_get_tmp.return_value = "/tmp/test.gridcal"
        mock_exists.return_value = True
        
        # Circuito sin el bus buscado
        mock_circuit = MagicMock()
        mock_circuit.buses = []
        
        mock_gce.open_file.return_value = mock_circuit
        
        # No debe fallar, solo loguear warning
        result = update_bus_status(1, False)
        
        assert result["message"] == "Bus status updated"

    @patch("services.bus.os.path.exists")
    @patch("services.bus.gce")
    @patch("services.bus.get_tmp_file_path")
    @patch("services.bus.repo_update_elements_by_bus_idtag")
    @patch("services.bus.repo_update_bus_status")
    @patch("services.bus.repo_get_bus_by_id")
    def test_update_bus_status_veragrid_error_continues(
        self, mock_repo_get, mock_repo_update, mock_repo_elements,
        mock_get_tmp, mock_gce, mock_exists, sample_bus_tuple
    ):
        """No debe fallar la petición si hay error en VeraGrid."""
        mock_repo_get.return_value = sample_bus_tuple
        mock_repo_update.return_value = (1, 1, "bus-001")
        mock_get_tmp.return_value = "/tmp/test.gridcal"
        mock_exists.return_value = True
        
        mock_gce.open_file.side_effect = Exception("VeraGrid error")
        
        # No debe fallar, solo loguear error
        result = update_bus_status(1, False)
        
        assert result["message"] == "Bus status updated"

    @patch("services.bus.gce")
    @patch("services.bus.get_tmp_file_path")
    @patch("services.bus.repo_update_elements_by_bus_idtag")
    @patch("services.bus.repo_update_bus_status")
    @patch("services.bus.repo_get_bus_by_id")
    def test_update_bus_status_with_dict_row(
        self, mock_repo_get, mock_repo_update, mock_repo_elements,
        mock_get_tmp, mock_gce, sample_bus_dict
    ):
        """Debe funcionar con filas en formato dict."""
        mock_repo_get.return_value = sample_bus_dict
        mock_repo_update.return_value = (1, 1, "bus-001")
        mock_get_tmp.return_value = None
        
        result = update_bus_status(1, True)
        
        assert result["message"] == "Bus status updated"
        assert result["bus_id"] == 1
        assert result["active"] is True

    @patch("services.bus.os.path.exists")
    @patch("services.bus.gce")
    @patch("services.bus.get_tmp_file_path")
    @patch("services.bus.repo_update_elements_by_bus_idtag")
    @patch("services.bus.repo_update_bus_status")
    @patch("services.bus.repo_get_bus_by_id")
    def test_update_bus_status_no_tmp_file_skips_veragrid(
        self, mock_repo_get, mock_repo_update, mock_repo_elements,
        mock_get_tmp, mock_gce, mock_exists, sample_bus_tuple
    ):
        """Debe saltar actualización VeraGrid si no hay archivo temporal."""
        mock_repo_get.return_value = sample_bus_tuple
        mock_repo_update.return_value = (1, 1, "bus-001")
        mock_get_tmp.return_value = None
        
        result = update_bus_status(1, False)
        
        assert result["message"] == "Bus status updated"
        mock_gce.open_file.assert_not_called()

    @patch("services.bus.os.path.exists")
    @patch("services.bus.gce")
    @patch("services.bus.get_tmp_file_path")
    @patch("services.bus.repo_update_elements_by_bus_idtag")
    @patch("services.bus.repo_update_bus_status")
    @patch("services.bus.repo_get_bus_by_id")
    def test_update_bus_status_tmp_file_not_exists_skips_veragrid(
        self, mock_repo_get, mock_repo_update, mock_repo_elements,
        mock_get_tmp, mock_gce, mock_exists, sample_bus_tuple
    ):
        """Debe saltar actualización VeraGrid si el archivo no existe."""
        mock_repo_get.return_value = sample_bus_tuple
        mock_repo_update.return_value = (1, 1, "bus-001")
        mock_get_tmp.return_value = "/tmp/test.gridcal"
        mock_exists.return_value = False
        
        result = update_bus_status(1, False)
        
        assert result["message"] == "Bus status updated"
        mock_gce.open_file.assert_not_called()
