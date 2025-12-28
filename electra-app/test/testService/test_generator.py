import sys
import os

# Añadir el directorio raíz de la aplicación al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import pytest
from unittest.mock import patch, MagicMock
from fastapi import HTTPException

from services.generator import get_generator, list_generators, update_generator_status


# =====================================================================
# Fixtures y datos de prueba
# =====================================================================

@pytest.fixture
def sample_generator_tuple():
    """Tuple que simula una fila de generator devuelta por el repositorio."""
    return (
        1,              # id
        1,              # grid_id
        "gen-001",      # idtag
        "Generator 1",  # name
        "G001",         # code
        "bus-001",      # bus_idtag
        True,           # active
        100.0,          # p
        1.0,            # vset
        -50.0,          # qmin
        50.0,           # qmax
        0.9,            # pf
        "rdfid-001",    # rdfid
        "action1",      # action
        "comment",      # comment
        "authority",    # modelling_authority
        "2020-01-01",   # commissioned_date
        None,           # decommissioned_date
        None,           # active_prof
        1000.0,         # mttf
        10.0,           # mttr
        100000.0,       # capex
        5000.0,         # opex
        "operating",    # build_status
        50.0,           # cost
        None,           # cost_prof
        "facility1",    # facility
        ["solar"],      # technologies
        True,           # scalable
        0.5,            # shift_key
        None,           # shift_key_prof
        -3.7,           # longitude
        40.4,           # latitude
        False,          # use_kw
        "conn1",        # conn
        "model1",       # rms_model
        0,              # bus_pos
        None,           # control_bus
        None,           # control_bus_prof
        None,           # p_prof
        0.0,            # pmin
        None,           # pmin_prof
        200.0,          # pmax
        None,           # pmax_prof
        False,          # srap_enabled
        None,           # srap_enabled_prof
        True,           # is_controlled
        None,           # pf_prof
        None,           # vset_prof
        100.0,          # snom
        None,           # qmin_prof
        None,           # qmax_prof
        False,          # use_reactive_power_curve
        None,           # q_curve
        0.01,           # r1
        0.1,            # x1
        0.02,           # r0
        0.2,            # x0
        0.015,          # r2
        0.15,           # x2
        0.0,            # cost2
        None,           # cost2_prof
        0.0,            # cost0
        None,           # cost0_prof
        100.0,          # startupcost
        50.0,           # shutdowncost
        1,              # mintimeup
        1,              # mintimedown
        10.0,           # rampup
        10.0,           # rampdown
        True,           # enabled_dispatch
        {"co2": 0.5},   # emissions
        ["gas"],        # fuels
    )


@pytest.fixture
def sample_generator_dict():
    """Diccionario que simula una fila de generator como dict."""
    return {
        "id": 1,
        "grid_id": 1,
        "idtag": "gen-001",
        "name": "Generator 1",
        "code": "G001",
        "bus_idtag": "bus-001",
        "active": True,
        "p": 100.0,
        "vset": 1.0,
        "qmin": -50.0,
        "qmax": 50.0,
        "pf": 0.9,
        "rdfid": "rdfid-001",
        "action": "action1",
        "comment": "comment",
        "modelling_authority": "authority",
        "commissioned_date": "2020-01-01",
        "decommissioned_date": None,
        "active_prof": None,
        "mttf": 1000.0,
        "mttr": 10.0,
        "capex": 100000.0,
        "opex": 5000.0,
        "build_status": "operating",
        "cost": 50.0,
        "cost_prof": None,
        "facility": "facility1",
        "technologies": ["solar"],
        "scalable": True,
        "shift_key": 0.5,
        "shift_key_prof": None,
        "longitude": -3.7,
        "latitude": 40.4,
        "use_kw": False,
        "conn": "conn1",
        "rms_model": "model1",
        "bus_pos": 0,
        "control_bus": None,
        "control_bus_prof": None,
        "p_prof": None,
        "pmin": 0.0,
        "pmin_prof": None,
        "pmax": 200.0,
        "pmax_prof": None,
        "srap_enabled": False,
        "srap_enabled_prof": None,
        "is_controlled": True,
        "pf_prof": None,
        "vset_prof": None,
        "snom": 100.0,
        "qmin_prof": None,
        "qmax_prof": None,
        "use_reactive_power_curve": False,
        "q_curve": None,
        "r1": 0.01,
        "x1": 0.1,
        "r0": 0.02,
        "x0": 0.2,
        "r2": 0.015,
        "x2": 0.15,
        "cost2": 0.0,
        "cost2_prof": None,
        "cost0": 0.0,
        "cost0_prof": None,
        "startupcost": 100.0,
        "shutdowncost": 50.0,
        "mintimeup": 1,
        "mintimedown": 1,
        "rampup": 10.0,
        "rampdown": 10.0,
        "enabled_dispatch": True,
        "emissions": {"co2": 0.5},
        "fuels": ["gas"],
    }


# =====================================================================
# Tests para get_generator()
# =====================================================================

class TestGetGenerator:
    """Tests para la función get_generator."""

    @patch("services.generator.get_generator_by_id")
    def test_get_generator_not_found_raises_404(self, mock_repo_get):
        """Debe lanzar HTTPException 404 cuando el generator no existe."""
        mock_repo_get.return_value = None
        
        with pytest.raises(HTTPException) as exc_info:
            get_generator(999)
        
        assert exc_info.value.status_code == 404
        assert exc_info.value.detail == "Generator not found"
        mock_repo_get.assert_called_once_with(999)

    @patch("services.generator.get_generator_by_id")
    def test_get_generator_with_tuple_row(self, mock_repo_get, sample_generator_tuple):
        """Debe devolver generator correctamente cuando la fila es tuple."""
        mock_repo_get.return_value = sample_generator_tuple
        
        result = get_generator(1)
        
        assert result["id"] == 1
        assert result["grid_id"] == 1
        assert result["idtag"] == "gen-001"
        assert result["name"] == "Generator 1"
        assert result["bus_idtag"] == "bus-001"
        assert result["active"] is True
        assert result["p"] == 100.0
        mock_repo_get.assert_called_once_with(1)

    @patch("services.generator.get_generator_by_id")
    def test_get_generator_with_dict_row(self, mock_repo_get, sample_generator_dict):
        """Debe devolver generator correctamente cuando la fila es dict."""
        mock_repo_get.return_value = sample_generator_dict
        
        result = get_generator(1)
        
        assert result["id"] == 1
        assert result["idtag"] == "gen-001"
        mock_repo_get.assert_called_once_with(1)

    @patch("services.generator.get_generator_by_id")
    def test_get_generator_returns_all_fields(self, mock_repo_get, sample_generator_tuple):
        """Debe devolver todos los campos del generator."""
        mock_repo_get.return_value = sample_generator_tuple
        
        result = get_generator(1)
        
        expected_fields = [
            "id", "grid_id", "idtag", "name", "code", "bus_idtag", "active",
            "p", "vset", "qmin", "qmax", "pf", "rdfid", "action", "comment",
            "modelling_authority", "commissioned_date", "decommissioned_date",
            "active_prof", "mttf", "mttr", "capex", "opex", "build_status",
            "cost", "cost_prof", "facility", "technologies", "scalable",
            "shift_key", "shift_key_prof", "longitude", "latitude", "use_kw",
            "conn", "rms_model", "bus_pos", "control_bus", "control_bus_prof",
            "p_prof", "pmin", "pmin_prof", "pmax", "pmax_prof", "srap_enabled",
            "srap_enabled_prof", "is_controlled", "pf_prof", "vset_prof", "snom",
            "qmin_prof", "qmax_prof", "use_reactive_power_curve", "q_curve",
            "r1", "x1", "r0", "x0", "r2", "x2", "cost2", "cost2_prof", "cost0",
            "cost0_prof", "startupcost", "shutdowncost", "mintimeup", "mintimedown",
            "rampup", "rampdown", "enabled_dispatch", "emissions", "fuels"
        ]
        
        for field in expected_fields:
            assert field in result


# =====================================================================
# Tests para list_generators()
# =====================================================================

class TestListGenerators:
    """Tests para la función list_generators."""

    @patch("services.generator.repo_list_generators")
    def test_list_generators_returns_empty_list_when_no_generators(self, mock_repo_list):
        """Debe devolver lista vacía cuando no hay generators."""
        mock_repo_list.return_value = None
        
        result = list_generators()
        
        assert result == []
        mock_repo_list.assert_called_once()

    @patch("services.generator.repo_list_generators")
    def test_list_generators_returns_empty_list_when_empty_rows(self, mock_repo_list):
        """Debe devolver lista vacía cuando rows está vacío."""
        mock_repo_list.return_value = []
        
        result = list_generators()
        
        assert result == []
        mock_repo_list.assert_called_once()

    @patch("services.generator.repo_list_generators")
    def test_list_generators_with_tuple_rows(self, mock_repo_list, sample_generator_tuple):
        """Debe normalizar correctamente filas en formato tuple."""
        mock_repo_list.return_value = [sample_generator_tuple]
        
        result = list_generators()
        
        assert len(result) == 1
        assert result[0]["id"] == 1
        assert result[0]["grid_id"] == 1
        assert result[0]["idtag"] == "gen-001"
        assert result[0]["name"] == "Generator 1"
        assert result[0]["active"] is True
        mock_repo_list.assert_called_once()

    @patch("services.generator.repo_list_generators")
    def test_list_generators_with_dict_rows(self, mock_repo_list, sample_generator_dict):
        """Debe manejar correctamente filas en formato dict."""
        mock_repo_list.return_value = [sample_generator_dict]
        
        result = list_generators()
        
        assert len(result) == 1
        assert result[0]["id"] == 1
        assert result[0]["idtag"] == "gen-001"

    @patch("services.generator.repo_list_generators")
    def test_list_generators_with_multiple_generators(self, mock_repo_list, sample_generator_tuple):
        """Debe manejar múltiples generators correctamente."""
        gen2_tuple = (
            2, 1, "gen-002", "Generator 2", "G002", "bus-002", True, 150.0, 1.0,
            -60.0, 60.0, 0.85, "rdfid-002", "action2", "comment2", "authority2",
            "2021-01-01", None, None, 1000.0, 10.0, 120000.0, 6000.0, "operating",
            55.0, None, "facility2", ["wind"], True, 0.6, None, -3.8, 40.5, False,
            "conn2", "model2", 0, None, None, None, 0.0, None, 250.0, None, False,
            None, True, None, None, 120.0, None, None, False, None, 0.01, 0.1,
            0.02, 0.2, 0.015, 0.15, 0.0, None, 0.0, None, 120.0, 60.0, 1, 1,
            12.0, 12.0, True, {"co2": 0.3}, ["gas"]
        )
        mock_repo_list.return_value = [sample_generator_tuple, gen2_tuple]
        
        result = list_generators()
        
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[1]["id"] == 2
        assert result[0]["idtag"] == "gen-001"
        assert result[1]["idtag"] == "gen-002"


# =====================================================================
# Tests para update_generator_status()
# =====================================================================

class TestUpdateGeneratorStatus:
    """Tests para la función update_generator_status."""

    @patch("services.generator.get_generator_by_id")
    def test_update_generator_status_not_found_raises_404(self, mock_repo_get):
        """Debe lanzar HTTPException 404 cuando el generator no existe."""
        mock_repo_get.return_value = None
        
        with pytest.raises(HTTPException) as exc_info:
            update_generator_status(999, False)
        
        assert exc_info.value.status_code == 404
        assert exc_info.value.detail == "Generator not found"

    @patch("services.generator.gce")
    @patch("services.generator.get_tmp_file_path")
    @patch("services.generator.repo_update_generator_status")
    @patch("services.generator.get_generator_by_id")
    def test_update_generator_status_activate(
        self, mock_repo_get, mock_repo_update, mock_get_tmp, mock_gce,
        sample_generator_tuple
    ):
        """Debe activar un generator correctamente."""
        mock_repo_get.return_value = sample_generator_tuple
        mock_repo_update.return_value = (1, 1, "gen-001")
        mock_get_tmp.return_value = None  # Sin archivo temporal
        
        result = update_generator_status(1, True)
        
        assert result["message"] == "Generator status updated"
        assert result["generator_id"] == 1
        assert result["active"] is True
        mock_repo_update.assert_called_once_with(1, True)

    @patch("services.generator.gce")
    @patch("services.generator.get_tmp_file_path")
    @patch("services.generator.repo_update_generator_status")
    @patch("services.generator.get_generator_by_id")
    def test_update_generator_status_deactivate(
        self, mock_repo_get, mock_repo_update, mock_get_tmp, mock_gce,
        sample_generator_tuple
    ):
        """Debe desactivar un generator correctamente."""
        mock_repo_get.return_value = sample_generator_tuple
        mock_repo_update.return_value = (1, 1, "gen-001")
        mock_get_tmp.return_value = None  # Sin archivo temporal
        
        result = update_generator_status(1, False)
        
        assert result["message"] == "Generator status updated"
        assert result["generator_id"] == 1
        assert result["active"] is False
        mock_repo_update.assert_called_once_with(1, False)

    @patch("services.generator.repo_update_generator_status")
    @patch("services.generator.get_generator_by_id")
    def test_update_generator_status_update_fails_raises_404(
        self, mock_repo_get, mock_repo_update, sample_generator_tuple
    ):
        """Debe lanzar 404 si la actualización en DB falla."""
        mock_repo_get.return_value = sample_generator_tuple
        mock_repo_update.return_value = None
        
        with pytest.raises(HTTPException) as exc_info:
            update_generator_status(1, False)
        
        assert exc_info.value.status_code == 404
        assert exc_info.value.detail == "Generator not found"

    @patch("services.generator.os.path.exists")
    @patch("services.generator.gce")
    @patch("services.generator.get_tmp_file_path")
    @patch("services.generator.repo_update_generator_status")
    @patch("services.generator.get_generator_by_id")
    def test_update_generator_status_updates_veragrid_file(
        self, mock_repo_get, mock_repo_update, mock_get_tmp, mock_gce,
        mock_exists, sample_generator_tuple
    ):
        """Debe actualizar el archivo VeraGrid cuando existe."""
        mock_repo_get.return_value = sample_generator_tuple
        mock_repo_update.return_value = (1, 1, "gen-001")
        mock_get_tmp.return_value = "/tmp/test.gridcal"
        mock_exists.return_value = True
        
        # Mock del generador en el circuito
        mock_gen = MagicMock()
        mock_gen.idtag = "gen-001"
        mock_gen.active = True
        
        mock_circuit = MagicMock()
        mock_circuit.generators = [mock_gen]
        
        mock_gce.open_file.return_value = mock_circuit
        
        result = update_generator_status(1, False)
        
        assert result["active"] is False
        mock_gce.open_file.assert_called_once_with("/tmp/test.gridcal")
        mock_gce.save_file.assert_called_once_with(mock_circuit, "/tmp/test.gridcal")
        assert mock_gen.active is False

    @patch("services.generator.os.path.exists")
    @patch("services.generator.gce")
    @patch("services.generator.get_tmp_file_path")
    @patch("services.generator.repo_update_generator_status")
    @patch("services.generator.get_generator_by_id")
    def test_update_generator_status_generator_not_in_circuit(
        self, mock_repo_get, mock_repo_update, mock_get_tmp, mock_gce,
        mock_exists, sample_generator_tuple
    ):
        """Debe continuar si el generator no se encuentra en el circuito VeraGrid."""
        mock_repo_get.return_value = sample_generator_tuple
        mock_repo_update.return_value = (1, 1, "gen-001")
        mock_get_tmp.return_value = "/tmp/test.gridcal"
        mock_exists.return_value = True
        
        # Circuito sin el generador buscado
        mock_other_gen = MagicMock()
        mock_other_gen.idtag = "gen-other"
        
        mock_circuit = MagicMock()
        mock_circuit.generators = [mock_other_gen]
        
        mock_gce.open_file.return_value = mock_circuit
        
        result = update_generator_status(1, False)
        
        assert result["message"] == "Generator status updated"
        mock_gce.save_file.assert_called_once()

    @patch("services.generator.os.path.exists")
    @patch("services.generator.gce")
    @patch("services.generator.get_tmp_file_path")
    @patch("services.generator.repo_update_generator_status")
    @patch("services.generator.get_generator_by_id")
    def test_update_generator_status_veragrid_error_continues(
        self, mock_repo_get, mock_repo_update, mock_get_tmp, mock_gce,
        mock_exists, sample_generator_tuple
    ):
        """No debe fallar la petición si hay error en VeraGrid."""
        mock_repo_get.return_value = sample_generator_tuple
        mock_repo_update.return_value = (1, 1, "gen-001")
        mock_get_tmp.return_value = "/tmp/test.gridcal"
        mock_exists.return_value = True
        
        mock_gce.open_file.side_effect = Exception("VeraGrid error")
        
        # No debe fallar, solo loguear error
        result = update_generator_status(1, False)
        
        assert result["message"] == "Generator status updated"

    @patch("services.generator.gce")
    @patch("services.generator.get_tmp_file_path")
    @patch("services.generator.repo_update_generator_status")
    @patch("services.generator.get_generator_by_id")
    def test_update_generator_status_with_dict_row(
        self, mock_repo_get, mock_repo_update, mock_get_tmp, mock_gce,
        sample_generator_dict
    ):
        """Debe funcionar con filas en formato dict."""
        mock_repo_get.return_value = sample_generator_dict
        mock_repo_update.return_value = (1, 1, "gen-001")
        mock_get_tmp.return_value = None
        
        result = update_generator_status(1, True)
        
        assert result["message"] == "Generator status updated"
        assert result["generator_id"] == 1
        assert result["active"] is True

    @patch("services.generator.gce")
    @patch("services.generator.get_tmp_file_path")
    @patch("services.generator.repo_update_generator_status")
    @patch("services.generator.get_generator_by_id")
    def test_update_generator_status_no_tmp_file_skips_veragrid(
        self, mock_repo_get, mock_repo_update, mock_get_tmp, mock_gce,
        sample_generator_tuple
    ):
        """Debe saltar actualización VeraGrid si no hay archivo temporal."""
        mock_repo_get.return_value = sample_generator_tuple
        mock_repo_update.return_value = (1, 1, "gen-001")
        mock_get_tmp.return_value = None
        
        result = update_generator_status(1, False)
        
        assert result["message"] == "Generator status updated"
        mock_gce.open_file.assert_not_called()

    @patch("services.generator.os.path.exists")
    @patch("services.generator.gce")
    @patch("services.generator.get_tmp_file_path")
    @patch("services.generator.repo_update_generator_status")
    @patch("services.generator.get_generator_by_id")
    def test_update_generator_status_tmp_file_not_exists_skips_veragrid(
        self, mock_repo_get, mock_repo_update, mock_get_tmp, mock_gce,
        mock_exists, sample_generator_tuple
    ):
        """Debe saltar actualización VeraGrid si el archivo no existe."""
        mock_repo_get.return_value = sample_generator_tuple
        mock_repo_update.return_value = (1, 1, "gen-001")
        mock_get_tmp.return_value = "/tmp/test.gridcal"
        mock_exists.return_value = False
        
        result = update_generator_status(1, False)
        
        assert result["message"] == "Generator status updated"
        mock_gce.open_file.assert_not_called()
