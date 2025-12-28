import sys
import os

# Añadir el directorio raíz de la aplicación al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import pytest
from unittest.mock import patch, MagicMock
from fastapi import HTTPException

from services.line import get_line, list_lines, update_line_status


# =====================================================================
# Fixtures y datos de prueba
# =====================================================================

@pytest.fixture
def sample_line_tuple():
    """Tuple que simula una fila de line devuelta por el repositorio."""
    return (
        1,              # id
        1,              # grid_id
        "line-001",     # idtag
        "Line 1",       # name
        "L001",         # code
        "bus-001",      # bus_from_idtag
        "bus-002",      # bus_to_idtag
        True,           # active
        0.01,           # r
        0.1,            # x
        0.001,          # b
        10.0,           # length
        "rdfid-001",    # rdfid
        "action1",      # action
        "comment",      # comment
        "authority",    # modelling_authority
        "2020-01-01",   # commissioned_date
        None,           # decommissioned_date
        None,           # active_prof
        True,           # reducible
        100.0,          # rate
        None,           # rate_prof
        1.0,            # contingency_factor
        None,           # contingency_factor_prof
        1.0,            # protection_rating_factor
        None,           # protection_rating_factor_prof
        True,           # monitor_loading
        1000.0,         # mttf
        10.0,           # mttr
        50.0,           # cost
        None,           # cost_prof
        "operating",    # build_status
        100000.0,       # capex
        5000.0,         # opex
        "group1",       # line_group
        "#FF0000",      # color
        "model1",       # rms_model
        0,              # bus_from_pos
        0,              # bus_to_pos
        0.02,           # r0
        0.2,            # x0
        0.002,          # b0
        0.015,          # r2
        0.15,           # x2
        0.0015,         # b2
        0.0,            # ys
        0.0,            # ysh
        0.01,           # tolerance
        1,              # circuit_idx
        20.0,           # temp_base
        25.0,           # temp_oper
        None,           # temp_oper_prof
        0.004,          # alpha
        0.01,           # r_fault
        0.1,            # x_fault
        0.5,            # fault_pos
        None,           # template
        None,           # locations
        None,           # possible_tower_types
        None,           # possible_underground_line_types
        None,           # possible_sequence_line_types
    )


@pytest.fixture
def sample_line_dict():
    """Diccionario que simula una fila de line como dict."""
    return {
        "id": 1,
        "grid_id": 1,
        "idtag": "line-001",
        "name": "Line 1",
        "code": "L001",
        "bus_from_idtag": "bus-001",
        "bus_to_idtag": "bus-002",
        "active": True,
        "r": 0.01,
        "x": 0.1,
        "b": 0.001,
        "length": 10.0,
        "rdfid": "rdfid-001",
        "action": "action1",
        "comment": "comment",
        "modelling_authority": "authority",
        "commissioned_date": "2020-01-01",
        "decommissioned_date": None,
        "active_prof": None,
        "reducible": True,
        "rate": 100.0,
        "rate_prof": None,
        "contingency_factor": 1.0,
        "contingency_factor_prof": None,
        "protection_rating_factor": 1.0,
        "protection_rating_factor_prof": None,
        "monitor_loading": True,
        "mttf": 1000.0,
        "mttr": 10.0,
        "cost": 50.0,
        "cost_prof": None,
        "build_status": "operating",
        "capex": 100000.0,
        "opex": 5000.0,
        "line_group": "group1",
        "color": "#FF0000",
        "rms_model": "model1",
        "bus_from_pos": 0,
        "bus_to_pos": 0,
        "r0": 0.02,
        "x0": 0.2,
        "b0": 0.002,
        "r2": 0.015,
        "x2": 0.15,
        "b2": 0.0015,
        "ys": 0.0,
        "ysh": 0.0,
        "tolerance": 0.01,
        "circuit_idx": 1,
        "temp_base": 20.0,
        "temp_oper": 25.0,
        "temp_oper_prof": None,
        "alpha": 0.004,
        "r_fault": 0.01,
        "x_fault": 0.1,
        "fault_pos": 0.5,
        "template": None,
        "locations": None,
        "possible_tower_types": None,
        "possible_underground_line_types": None,
        "possible_sequence_line_types": None,
    }


# =====================================================================
# Tests para get_line()
# =====================================================================

class TestGetLine:
    """Tests para la función get_line."""

    @patch("services.line.get_line_by_id")
    def test_get_line_not_found_raises_404(self, mock_repo_get):
        """Debe lanzar HTTPException 404 cuando la line no existe."""
        mock_repo_get.return_value = None
        
        with pytest.raises(HTTPException) as exc_info:
            get_line(999)
        
        assert exc_info.value.status_code == 404
        assert exc_info.value.detail == "Line not found"
        mock_repo_get.assert_called_once_with(999)

    @patch("services.line.get_line_by_id")
    def test_get_line_with_tuple_row(self, mock_repo_get, sample_line_tuple):
        """Debe devolver line correctamente cuando la fila es tuple."""
        mock_repo_get.return_value = sample_line_tuple
        
        result = get_line(1)
        
        assert result["id"] == 1
        assert result["grid_id"] == 1
        assert result["idtag"] == "line-001"
        assert result["name"] == "Line 1"
        assert result["bus_from_idtag"] == "bus-001"
        assert result["bus_to_idtag"] == "bus-002"
        assert result["active"] is True
        assert result["r"] == 0.01
        mock_repo_get.assert_called_once_with(1)

    @patch("services.line.get_line_by_id")
    def test_get_line_with_dict_row(self, mock_repo_get, sample_line_dict):
        """Debe devolver line correctamente cuando la fila es dict."""
        mock_repo_get.return_value = sample_line_dict
        
        result = get_line(1)
        
        assert result["id"] == 1
        assert result["idtag"] == "line-001"
        mock_repo_get.assert_called_once_with(1)

    @patch("services.line.get_line_by_id")
    def test_get_line_returns_all_fields(self, mock_repo_get, sample_line_tuple):
        """Debe devolver todos los campos de la line."""
        mock_repo_get.return_value = sample_line_tuple
        
        result = get_line(1)
        
        expected_fields = [
            "id", "grid_id", "idtag", "name", "code", "bus_from_idtag", "bus_to_idtag",
            "active", "r", "x", "b", "length", "rdfid", "action", "comment",
            "modelling_authority", "commissioned_date", "decommissioned_date",
            "active_prof", "reducible", "rate", "rate_prof", "contingency_factor",
            "contingency_factor_prof", "protection_rating_factor",
            "protection_rating_factor_prof", "monitor_loading", "mttf", "mttr",
            "cost", "cost_prof", "build_status", "capex", "opex", "line_group",
            "color", "rms_model", "bus_from_pos", "bus_to_pos", "r0", "x0", "b0",
            "r2", "x2", "b2", "ys", "ysh", "tolerance", "circuit_idx", "temp_base",
            "temp_oper", "temp_oper_prof", "alpha", "r_fault", "x_fault", "fault_pos",
            "template", "locations", "possible_tower_types",
            "possible_underground_line_types", "possible_sequence_line_types"
        ]
        
        for field in expected_fields:
            assert field in result


# =====================================================================
# Tests para list_lines()
# =====================================================================

class TestListLines:
    """Tests para la función list_lines."""

    @patch("services.line.repo_list_lines")
    def test_list_lines_returns_empty_list_when_no_lines(self, mock_repo_list):
        """Debe devolver lista vacía cuando no hay lines."""
        mock_repo_list.return_value = None
        
        result = list_lines()
        
        assert result == []
        mock_repo_list.assert_called_once()

    @patch("services.line.repo_list_lines")
    def test_list_lines_returns_empty_list_when_empty_rows(self, mock_repo_list):
        """Debe devolver lista vacía cuando rows está vacío."""
        mock_repo_list.return_value = []
        
        result = list_lines()
        
        assert result == []
        mock_repo_list.assert_called_once()

    @patch("services.line.repo_list_lines")
    def test_list_lines_with_tuple_rows(self, mock_repo_list, sample_line_tuple):
        """Debe normalizar correctamente filas en formato tuple."""
        mock_repo_list.return_value = [sample_line_tuple]
        
        result = list_lines()
        
        assert len(result) == 1
        assert result[0]["id"] == 1
        assert result[0]["grid_id"] == 1
        assert result[0]["idtag"] == "line-001"
        assert result[0]["name"] == "Line 1"
        assert result[0]["active"] is True
        mock_repo_list.assert_called_once()

    @patch("services.line.repo_list_lines")
    def test_list_lines_with_dict_rows(self, mock_repo_list, sample_line_dict):
        """Debe manejar correctamente filas en formato dict."""
        mock_repo_list.return_value = [sample_line_dict]
        
        result = list_lines()
        
        assert len(result) == 1
        assert result[0]["id"] == 1
        assert result[0]["idtag"] == "line-001"

    @patch("services.line.repo_list_lines")
    def test_list_lines_with_multiple_lines(self, mock_repo_list, sample_line_tuple):
        """Debe manejar múltiples lines correctamente."""
        line2_tuple = (
            2, 1, "line-002", "Line 2", "L002", "bus-002", "bus-003", True,
            0.02, 0.2, 0.002, 15.0, "rdfid-002", "action2", "comment2", "authority2",
            "2021-01-01", None, None, True, 150.0, None, 1.0, None, 1.0, None, True,
            1000.0, 10.0, 60.0, None, "operating", 120000.0, 6000.0, "group2",
            "#00FF00", "model2", 0, 0, 0.03, 0.3, 0.003, 0.02, 0.2, 0.002, 0.0, 0.0,
            0.01, 2, 20.0, 25.0, None, 0.004, 0.02, 0.2, 0.5, None, None, None, None, None
        )
        mock_repo_list.return_value = [sample_line_tuple, line2_tuple]
        
        result = list_lines()
        
        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[1]["id"] == 2
        assert result[0]["idtag"] == "line-001"
        assert result[1]["idtag"] == "line-002"


# =====================================================================
# Tests para update_line_status()
# =====================================================================

class TestUpdateLineStatus:
    """Tests para la función update_line_status."""

    @patch("services.line.get_line_by_id")
    def test_update_line_status_not_found_raises_404(self, mock_repo_get):
        """Debe lanzar HTTPException 404 cuando la line no existe."""
        mock_repo_get.return_value = None
        
        with pytest.raises(HTTPException) as exc_info:
            update_line_status(999, False)
        
        assert exc_info.value.status_code == 404
        assert exc_info.value.detail == "Line not found"

    @patch("services.line.gce")
    @patch("services.line.get_tmp_file_path")
    @patch("services.line.repo_update_line_status")
    @patch("services.line.get_line_by_id")
    def test_update_line_status_activate(
        self, mock_repo_get, mock_repo_update, mock_get_tmp, mock_gce,
        sample_line_tuple
    ):
        """Debe activar una line correctamente."""
        mock_repo_get.return_value = sample_line_tuple
        mock_repo_update.return_value = (1, 1, "line-001")
        mock_get_tmp.return_value = None  # Sin archivo temporal
        
        result = update_line_status(1, True)
        
        assert result["message"] == "Line status updated"
        assert result["line_id"] == 1
        assert result["active"] is True
        mock_repo_update.assert_called_once_with(1, True)

    @patch("services.line.gce")
    @patch("services.line.get_tmp_file_path")
    @patch("services.line.repo_update_line_status")
    @patch("services.line.get_line_by_id")
    def test_update_line_status_deactivate(
        self, mock_repo_get, mock_repo_update, mock_get_tmp, mock_gce,
        sample_line_tuple
    ):
        """Debe desactivar una line correctamente."""
        mock_repo_get.return_value = sample_line_tuple
        mock_repo_update.return_value = (1, 1, "line-001")
        mock_get_tmp.return_value = None  # Sin archivo temporal
        
        result = update_line_status(1, False)
        
        assert result["message"] == "Line status updated"
        assert result["line_id"] == 1
        assert result["active"] is False
        mock_repo_update.assert_called_once_with(1, False)

    @patch("services.line.repo_update_line_status")
    @patch("services.line.get_line_by_id")
    def test_update_line_status_update_fails_raises_404(
        self, mock_repo_get, mock_repo_update, sample_line_tuple
    ):
        """Debe lanzar 404 si la actualización en DB falla."""
        mock_repo_get.return_value = sample_line_tuple
        mock_repo_update.return_value = None
        
        with pytest.raises(HTTPException) as exc_info:
            update_line_status(1, False)
        
        assert exc_info.value.status_code == 404
        assert exc_info.value.detail == "Line not found"

    @patch("services.line.os.path.exists")
    @patch("services.line.gce")
    @patch("services.line.get_tmp_file_path")
    @patch("services.line.repo_update_line_status")
    @patch("services.line.get_line_by_id")
    def test_update_line_status_updates_veragrid_file(
        self, mock_repo_get, mock_repo_update, mock_get_tmp, mock_gce,
        mock_exists, sample_line_tuple
    ):
        """Debe actualizar el archivo VeraGrid cuando existe."""
        mock_repo_get.return_value = sample_line_tuple
        mock_repo_update.return_value = (1, 1, "line-001")
        mock_get_tmp.return_value = "/tmp/test.gridcal"
        mock_exists.return_value = True
        
        # Mock de la línea en el circuito
        mock_line = MagicMock()
        mock_line.idtag = "line-001"
        mock_line.active = True
        
        mock_circuit = MagicMock()
        mock_circuit.lines = [mock_line]
        
        mock_gce.open_file.return_value = mock_circuit
        
        result = update_line_status(1, False)
        
        assert result["active"] is False
        mock_gce.open_file.assert_called_once_with("/tmp/test.gridcal")
        mock_gce.save_file.assert_called_once_with(mock_circuit, "/tmp/test.gridcal")
        assert mock_line.active is False

    @patch("services.line.os.path.exists")
    @patch("services.line.gce")
    @patch("services.line.get_tmp_file_path")
    @patch("services.line.repo_update_line_status")
    @patch("services.line.get_line_by_id")
    def test_update_line_status_line_not_in_circuit(
        self, mock_repo_get, mock_repo_update, mock_get_tmp, mock_gce,
        mock_exists, sample_line_tuple
    ):
        """Debe continuar si la line no se encuentra en el circuito VeraGrid."""
        mock_repo_get.return_value = sample_line_tuple
        mock_repo_update.return_value = (1, 1, "line-001")
        mock_get_tmp.return_value = "/tmp/test.gridcal"
        mock_exists.return_value = True
        
        # Circuito sin la línea buscada
        mock_other_line = MagicMock()
        mock_other_line.idtag = "line-other"
        
        mock_circuit = MagicMock()
        mock_circuit.lines = [mock_other_line]
        
        mock_gce.open_file.return_value = mock_circuit
        
        result = update_line_status(1, False)
        
        assert result["message"] == "Line status updated"
        mock_gce.save_file.assert_called_once()

    @patch("services.line.os.path.exists")
    @patch("services.line.gce")
    @patch("services.line.get_tmp_file_path")
    @patch("services.line.repo_update_line_status")
    @patch("services.line.get_line_by_id")
    def test_update_line_status_veragrid_error_continues(
        self, mock_repo_get, mock_repo_update, mock_get_tmp, mock_gce,
        mock_exists, sample_line_tuple
    ):
        """No debe fallar la petición si hay error en VeraGrid."""
        mock_repo_get.return_value = sample_line_tuple
        mock_repo_update.return_value = (1, 1, "line-001")
        mock_get_tmp.return_value = "/tmp/test.gridcal"
        mock_exists.return_value = True
        
        mock_gce.open_file.side_effect = Exception("VeraGrid error")
        
        # No debe fallar, solo loguear error
        result = update_line_status(1, False)
        
        assert result["message"] == "Line status updated"

    @patch("services.line.gce")
    @patch("services.line.get_tmp_file_path")
    @patch("services.line.repo_update_line_status")
    @patch("services.line.get_line_by_id")
    def test_update_line_status_with_dict_row(
        self, mock_repo_get, mock_repo_update, mock_get_tmp, mock_gce,
        sample_line_dict
    ):
        """Debe funcionar con filas en formato dict."""
        mock_repo_get.return_value = sample_line_dict
        mock_repo_update.return_value = (1, 1, "line-001")
        mock_get_tmp.return_value = None
        
        result = update_line_status(1, True)
        
        assert result["message"] == "Line status updated"
        assert result["line_id"] == 1
        assert result["active"] is True

    @patch("services.line.gce")
    @patch("services.line.get_tmp_file_path")
    @patch("services.line.repo_update_line_status")
    @patch("services.line.get_line_by_id")
    def test_update_line_status_no_tmp_file_skips_veragrid(
        self, mock_repo_get, mock_repo_update, mock_get_tmp, mock_gce,
        sample_line_tuple
    ):
        """Debe saltar actualización VeraGrid si no hay archivo temporal."""
        mock_repo_get.return_value = sample_line_tuple
        mock_repo_update.return_value = (1, 1, "line-001")
        mock_get_tmp.return_value = None
        
        result = update_line_status(1, False)
        
        assert result["message"] == "Line status updated"
        mock_gce.open_file.assert_not_called()

    @patch("services.line.os.path.exists")
    @patch("services.line.gce")
    @patch("services.line.get_tmp_file_path")
    @patch("services.line.repo_update_line_status")
    @patch("services.line.get_line_by_id")
    def test_update_line_status_tmp_file_not_exists_skips_veragrid(
        self, mock_repo_get, mock_repo_update, mock_get_tmp, mock_gce,
        mock_exists, sample_line_tuple
    ):
        """Debe saltar actualización VeraGrid si el archivo no existe."""
        mock_repo_get.return_value = sample_line_tuple
        mock_repo_update.return_value = (1, 1, "line-001")
        mock_get_tmp.return_value = "/tmp/test.gridcal"
        mock_exists.return_value = False
        
        result = update_line_status(1, False)
        
        assert result["message"] == "Line status updated"
        mock_gce.open_file.assert_not_called()
