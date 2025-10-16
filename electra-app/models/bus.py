from pydantic import BaseModel, ConfigDict

class Bus(BaseModel):
    model_config = ConfigDict(extra="ignore")
    grid_id: int | None = None
    idtag: str
    name: str
    code: str
    rdfid: str
    action: str
    comment: str
    modelling_authority: str
    commissioned_date: int
    decommissioned_date: int
    active: bool

    is_slack: bool
    is_dc: bool
    graphic_type: str

    Vnom: float
    Vm0: float
    Va0: float
    Vmin: float
    Vmax: float
    Vm_cost: float
    angle_min: float
    angle_max: float
    angle_cost: float

    r_fault: float
    x_fault: float

    x: float
    y: float
    h: float
    w: float

    country: str
    area: str
    zone: str
    substation: str
    voltage_level: str
    bus_bar: str

    longitude: float
    latitude: float
    ph_a: bool
    ph_b: bool
    ph_c: bool
    ph_n: bool
    is_grounded: bool
