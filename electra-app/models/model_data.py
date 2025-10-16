from pydantic import BaseModel, ConfigDict
from typing import Any, List, Optional
from .bus import Bus
from .load import Load
from .generator import Generator
from .shunt import Shunt
from .transformer2w import Transformer2W
from .line import Line
from .time_axis import TimeAxis
from .circuit import Circuit
from .contingency import Contingency, ContingencyGroup

class ModelData(BaseModel):
    model_config = ConfigDict(extra="ignore")
    # Taxonomy/metadata lists kept as Any
    modelling_authority: List[Any] = []
    area: List[Any] = []
    zone: List[Any] = []
    country: List[Any] = []
    community: List[Any] = []
    region: List[Any] = []
    municipality: List[Any] = []
    substation: List[Any] = []
    voltage_level: List[Any] = []
    technology: List[Any] = []
    fuel: List[Any] = []
    emission: List[Any] = []
    facility: List[Any] = []

    bus: List[Bus] = []
    bus_bar: List[Any] = []
    load: List[Load] = []
    static_generator: List[Any] = []
    battery: List[Any] = []
    generator: List[Generator] = []
    shunt: List[Shunt] = []
    linear_shunt: List[Any] = []
    external_grid: List[Any] = []
    current_injection: List[Any] = []
    wires: List[Any] = []
    overhead_line_types: List[Any] = []
    underground_cable_types: List[Any] = []
    sequence_line_types: List[Any] = []
    transformer_types: List[Any] = []
    branch_group: List[Any] = []
    transformer2w: List[Transformer2W] = []
    windings: List[Any] = []
    transformer3w: List[Any] = []
    line: List[Line] = []
    dc_line: List[Any] = []
    hvdc: List[Any] = []
    vsc: List[Any] = []
    upfc: List[Any] = []
    series_reactance: List[Any] = []
    switch: List[Any] = []

    contingency_group: List[ContingencyGroup] = []
    contingency: List[Contingency] = []

    remedial_action_group: List[Any] = []
    remedial_action: List[Any] = []
    investments_group: List[Any] = []
    investment: List[Any] = []

    fluid_node: List[Any] = []
    fluid_path: List[Any] = []
    fluid_turbine: List[Any] = []
    fluid_pump: List[Any] = []
    fluid_p2x: List[Any] = []

    time: Optional[TimeAxis] = None
    circuit: Optional[Circuit] = None
