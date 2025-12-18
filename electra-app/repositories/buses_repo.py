from psycopg2.extras import execute_values, Json
from db.db import get_conn


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS buses (
                id SERIAL PRIMARY KEY,
                grid_id INTEGER NOT NULL REFERENCES grids(id) ON DELETE CASCADE,
                idtag TEXT NOT NULL,
                name TEXT,
                code TEXT,
                vnom DOUBLE PRECISION,
                vm0 DOUBLE PRECISION,
                va0 DOUBLE PRECISION,
                vmin DOUBLE PRECISION,
                vmax DOUBLE PRECISION,
                vm_cost DOUBLE PRECISION,
                angle_min DOUBLE PRECISION,
                angle_max DOUBLE PRECISION,
                angle_cost DOUBLE PRECISION,
                r_fault DOUBLE PRECISION,
                x_fault DOUBLE PRECISION,
                x DOUBLE PRECISION,
                y DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                latitude DOUBLE PRECISION,
                is_slack BOOLEAN,
                active BOOLEAN,
                is_dc BOOLEAN,
                graphic_type TEXT,
                h DOUBLE PRECISION,
                w DOUBLE PRECISION,
                country TEXT,
                area TEXT,
                zone TEXT,
                substation TEXT,
                voltage_level TEXT,
                bus_bar TEXT,
                ph_a BOOLEAN,
                ph_b BOOLEAN,
                ph_c BOOLEAN,
                ph_n BOOLEAN,
                is_grounded BOOLEAN,
                active_prof JSONB,
                vmin_prof JSONB,
                vmax_prof JSONB,
                UNIQUE (grid_id, idtag)
            );
            """
        )
        # Add columns if they don't exist (for existing databases)
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS grid_id INTEGER;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS vmin DOUBLE PRECISION;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS vmax DOUBLE PRECISION;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS vm_cost DOUBLE PRECISION;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS angle_min DOUBLE PRECISION;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS angle_max DOUBLE PRECISION;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS angle_cost DOUBLE PRECISION;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS r_fault DOUBLE PRECISION;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS x_fault DOUBLE PRECISION;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS active BOOLEAN;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS is_dc BOOLEAN;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS graphic_type TEXT;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS h DOUBLE PRECISION;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS w DOUBLE PRECISION;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS country TEXT;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS area TEXT;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS zone TEXT;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS substation TEXT;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS voltage_level TEXT;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS bus_bar TEXT;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS ph_a BOOLEAN;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS ph_b BOOLEAN;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS ph_c BOOLEAN;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS ph_n BOOLEAN;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS is_grounded BOOLEAN;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS active_prof JSONB;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS vmin_prof JSONB;")
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS vmax_prof JSONB;")
        cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                WHERE c.conname = 'fk_buses_grid'
                  AND t.relname = 'buses'
            ) THEN
                ALTER TABLE buses
                ADD CONSTRAINT fk_buses_grid
                FOREIGN KEY (grid_id) REFERENCES grids(id) ON DELETE CASCADE;
            END IF;
        END$$;
        """)
    conn.commit()


def upsert(conn, grid_id: int, buses: list[dict]) -> int:
    rows = []
    for b in (buses or []):
        if not b.get("idtag"):
            continue
        ap = b.get("active_prof")
        vminp = b.get("Vmin_prof")
        vmaxp = b.get("Vmax_prof")
        rows.append(
            (
                grid_id,
                b.get("idtag"),
                b.get("name"),
                b.get("code"),
                b.get("Vnom"),
                b.get("Vm0"),
                b.get("Va0"),
                b.get("Vmin"),
                b.get("Vmax"),
                b.get("Vm_cost"),
                b.get("angle_min"),
                b.get("angle_max"),
                b.get("angle_cost"),
                b.get("r_fault"),
                b.get("x_fault"),
                b.get("x"),
                b.get("y"),
                b.get("longitude"),
                b.get("latitude"),
                bool(b.get("is_slack", False)),
                bool(b.get("active", False)),
                bool(b.get("is_dc", False)),
                b.get("graphic_type"),
                b.get("h"),
                b.get("w"),
                b.get("country"),
                b.get("area"),
                b.get("zone"),
                b.get("substation"),
                b.get("voltage_level"),
                b.get("bus_bar"),
                bool(b.get("ph_a", False)),
                bool(b.get("ph_b", False)),
                bool(b.get("ph_c", False)),
                bool(b.get("ph_n", False)),
                bool(b.get("is_grounded", False)),
                (Json(ap) if ap is not None else None),
                (Json(vminp) if vminp is not None else None),
                (Json(vmaxp) if vmaxp is not None else None),
            )
        )
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO buses (
                grid_id, idtag, name, code, vnom, vm0, va0, vmin, vmax, vm_cost, angle_min, angle_max, angle_cost, r_fault, x_fault,
                x, y, longitude, latitude, is_slack, active, is_dc, graphic_type, h, w, country, area, zone, substation, voltage_level,
                bus_bar, ph_a, ph_b, ph_c, ph_n, is_grounded, active_prof, vmin_prof, vmax_prof
            ) VALUES %s
            ON CONFLICT (grid_id, idtag) DO UPDATE SET
                name = EXCLUDED.name,
                code = EXCLUDED.code,
                vnom = EXCLUDED.vnom,
                vm0 = EXCLUDED.vm0,
                va0 = EXCLUDED.va0,
                vmin = EXCLUDED.vmin,
                vmax = EXCLUDED.vmax,
                vm_cost = EXCLUDED.vm_cost,
                angle_min = EXCLUDED.angle_min,
                angle_max = EXCLUDED.angle_max,
                angle_cost = EXCLUDED.angle_cost,
                r_fault = EXCLUDED.r_fault,
                x_fault = EXCLUDED.x_fault,
                x = EXCLUDED.x,
                y = EXCLUDED.y,
                longitude = EXCLUDED.longitude,
                latitude = EXCLUDED.latitude,
                is_slack = EXCLUDED.is_slack,
                active = EXCLUDED.active,
                is_dc = EXCLUDED.is_dc,
                graphic_type = EXCLUDED.graphic_type,
                h = EXCLUDED.h,
                w = EXCLUDED.w,
                country = EXCLUDED.country,
                area = EXCLUDED.area,
                zone = EXCLUDED.zone,
                substation = EXCLUDED.substation,
                voltage_level = EXCLUDED.voltage_level,
                bus_bar = EXCLUDED.bus_bar,
                ph_a = EXCLUDED.ph_a,
                ph_b = EXCLUDED.ph_b,
                ph_c = EXCLUDED.ph_c,
                ph_n = EXCLUDED.ph_n,
                is_grounded = EXCLUDED.is_grounded,
                active_prof = EXCLUDED.active_prof,
                vmin_prof = EXCLUDED.vmin_prof,
                vmax_prof = EXCLUDED.vmax_prof
            """,
            rows,
        )
    return len(rows)


def get_bus_by_id(bus_id: int):
    """Return a single bus row by its primary key id, or None if not found."""
    conn = get_conn()
    try:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, grid_id, idtag, name, code, vnom, vm0, va0, vmin, vmax, vm_cost, angle_min, angle_max, angle_cost, r_fault, x_fault,
                       x, y, longitude, latitude, is_slack, active, is_dc, graphic_type, h, w, country, area, zone, substation, voltage_level,
                       bus_bar, ph_a, ph_b, ph_c, ph_n, is_grounded, active_prof, vmin_prof, vmax_prof
                FROM buses
                WHERE id = %s;
                """,
                (bus_id,),
            )
            row = cur.fetchone()
            return row
    finally:
        conn.close()


def list_buses():
    """Return all buses as a list of rows (dicts when using RealDictCursor)."""
    conn = get_conn()
    try:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, grid_id, idtag, name, code, vnom, vm0, va0, vmin, vmax, vm_cost, angle_min, angle_max, angle_cost, r_fault, x_fault,
                       x, y, longitude, latitude, is_slack, active, is_dc, graphic_type, h, w, country, area, zone, substation, voltage_level,
                       bus_bar, ph_a, ph_b, ph_c, ph_n, is_grounded, active_prof, vmin_prof, vmax_prof
                FROM buses
                ORDER BY id;
                """
            )
            rows = cur.fetchall()
            return rows
    finally:
        conn.close()


def update_bus_status(bus_id: int, active: bool):
    """Update the active status of a bus."""
    conn = get_conn()
    try:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE buses
                SET active = %s
                WHERE id = %s
                RETURNING id, grid_id, idtag;
                """,
                (active, bus_id),
            )
            row = cur.fetchone()
            if not row:
                return None
        conn.commit()
        return row
    finally:
        conn.close()


def update_elements_by_bus_idtag(grid_id: int, bus_idtag: str, active: bool):
    """Update active status of generators, loads, and shunts connected to a bus (cascading deactivation).
    Note: Lines and transformers are NOT affected."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Update generators connected to this bus
            cur.execute(
                """
                UPDATE generators
                SET active = %s
                WHERE grid_id = %s AND bus_idtag = %s;
                """,
                (active, grid_id, bus_idtag),
            )
            
            # Update loads connected to this bus
            cur.execute(
                """
                UPDATE loads
                SET active = %s
                WHERE grid_id = %s AND bus_idtag = %s;
                """,
                (active, grid_id, bus_idtag),
            )
            
            # Update shunts connected to this bus
            cur.execute(
                """
                UPDATE shunts
                SET active = %s
                WHERE grid_id = %s AND bus_idtag = %s;
                """,
                (active, grid_id, bus_idtag),
            )
        conn.commit()
    finally:
        conn.close()
