from psycopg2.extras import execute_values, Json
from db.db import get_conn


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS generators (
                id SERIAL PRIMARY KEY,
                grid_id INTEGER NOT NULL REFERENCES grids(id) ON DELETE CASCADE,
                idtag TEXT NOT NULL,
                name TEXT,
                code TEXT,
                bus_idtag TEXT NOT NULL,
                active BOOLEAN,
                p DOUBLE PRECISION,
                vset DOUBLE PRECISION,
                qmin DOUBLE PRECISION,
                qmax DOUBLE PRECISION,
                pf DOUBLE PRECISION,
                UNIQUE (grid_id, idtag),
                FOREIGN KEY (grid_id, bus_idtag) REFERENCES buses(grid_id, idtag) ON DELETE CASCADE
            );
            """
        )
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS grid_id INTEGER;")
        cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                WHERE c.conname = 'fk_generators_grid'
                  AND t.relname = 'generators'
            ) THEN
                ALTER TABLE generators
                ADD CONSTRAINT fk_generators_grid
                FOREIGN KEY (grid_id) REFERENCES grids(id) ON DELETE CASCADE;
            END IF;
        END$$;
        """)
        
        # Add extended fields
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS rdfid TEXT;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS action TEXT;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS comment TEXT;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS modelling_authority TEXT;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS commissioned_date BIGINT;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS decommissioned_date BIGINT;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS active_prof JSONB;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS mttf DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS mttr DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS capex DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS opex DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS build_status TEXT;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS cost DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS cost_prof JSONB;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS facility TEXT;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS technologies JSONB;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS scalable BOOLEAN;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS shift_key DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS shift_key_prof JSONB;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS use_kw BOOLEAN;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS conn TEXT;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS rms_model JSONB;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS bus_pos INTEGER;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS control_bus TEXT;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS control_bus_prof JSONB;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS p_prof JSONB;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS pmin DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS pmin_prof JSONB;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS pmax DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS pmax_prof JSONB;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS srap_enabled BOOLEAN;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS srap_enabled_prof JSONB;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS is_controlled BOOLEAN;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS pf_prof JSONB;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS vset_prof JSONB;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS snom DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS qmin_prof JSONB;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS qmax_prof JSONB;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS use_reactive_power_curve BOOLEAN;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS q_curve JSONB;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS r1 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS x1 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS r0 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS x0 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS r2 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS x2 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS cost2 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS cost2_prof JSONB;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS cost0 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS cost0_prof JSONB;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS startupcost DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS shutdowncost DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS mintimeup DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS mintimedown DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS rampup DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS rampdown DOUBLE PRECISION;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS enabled_dispatch BOOLEAN;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS emissions JSONB;")
        cur.execute("ALTER TABLE generators ADD COLUMN IF NOT EXISTS fuels JSONB;")
    conn.commit()


def upsert(conn, grid_id: int, generators: list[dict]) -> int:
    def adapt(val):
        if isinstance(val, dict) or isinstance(val, list):
            return Json(val)
        return val
    
    rows = [
        (
            grid_id,
            g.get("idtag"),
            g.get("name"),
            g.get("code"),
            g.get("bus"),
            bool(g.get("active", True)),
            g.get("P"),
            g.get("Vset"),
            g.get("Qmin"),
            g.get("Qmax"),
            g.get("Pf"),
            g.get("rdfid"),
            g.get("action"),
            g.get("comment"),
            g.get("modelling_authority"),
            g.get("commissioned_date"),
            g.get("decommissioned_date"),
            adapt(g.get("active_prof")),
            g.get("mttf"),
            g.get("mttr"),
            g.get("capex"),
            g.get("opex"),
            g.get("build_status"),
            g.get("Cost"),
            adapt(g.get("Cost_prof")),
            g.get("facility"),
            adapt(g.get("technologies")),
            g.get("scalable"),
            g.get("shift_key"),
            adapt(g.get("shift_key_prof")),
            g.get("longitude"),
            g.get("latitude"),
            g.get("use_kw"),
            g.get("conn"),
            adapt(g.get("rms_model")),
            g.get("bus_pos"),
            g.get("control_bus"),
            adapt(g.get("control_bus_prof")),
            adapt(g.get("P_prof")),
            g.get("Pmin"),
            adapt(g.get("Pmin_prof")),
            g.get("Pmax"),
            adapt(g.get("Pmax_prof")),
            g.get("srap_enabled"),
            adapt(g.get("srap_enabled_prof")),
            g.get("is_controlled"),
            adapt(g.get("Pf_prof")),
            adapt(g.get("Vset_prof")),
            g.get("Snom"),
            adapt(g.get("Qmin_prof")),
            adapt(g.get("Qmax_prof")),
            g.get("use_reactive_power_curve"),
            adapt(g.get("q_curve")),
            g.get("R1"),
            g.get("X1"),
            g.get("R0"),
            g.get("X0"),
            g.get("R2"),
            g.get("X2"),
            g.get("Cost2"),
            adapt(g.get("Cost2_prof")),
            g.get("Cost0"),
            adapt(g.get("Cost0_prof")),
            g.get("StartupCost"),
            g.get("ShutdownCost"),
            g.get("MinTimeUp"),
            g.get("MinTimeDown"),
            g.get("RampUp"),
            g.get("RampDown"),
            g.get("enabled_dispatch"),
            adapt(g.get("emissions")),
            adapt(g.get("fuels")),
        )
        for g in (generators or [])
        if g.get("idtag") and g.get("bus")
    ]
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO generators (
                grid_id, idtag, name, code, bus_idtag, active, p, vset, qmin, qmax, pf,
                rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
                active_prof, mttf, mttr, capex, opex, build_status, cost, cost_prof, facility,
                technologies, scalable, shift_key, shift_key_prof, longitude, latitude, use_kw,
                conn, rms_model, bus_pos, control_bus, control_bus_prof, p_prof, pmin, pmin_prof,
                pmax, pmax_prof, srap_enabled, srap_enabled_prof, is_controlled, pf_prof, vset_prof,
                snom, qmin_prof, qmax_prof, use_reactive_power_curve, q_curve, r1, x1, r0, x0, r2, x2,
                cost2, cost2_prof, cost0, cost0_prof, startupcost, shutdowncost, mintimeup, mintimedown,
                rampup, rampdown, enabled_dispatch, emissions, fuels
            ) VALUES %s
            ON CONFLICT (grid_id, idtag) DO UPDATE SET
                name = EXCLUDED.name,
                code = EXCLUDED.code,
                bus_idtag = EXCLUDED.bus_idtag,
                active = EXCLUDED.active,
                p = EXCLUDED.p,
                vset = EXCLUDED.vset,
                qmin = EXCLUDED.qmin,
                qmax = EXCLUDED.qmax,
                pf = EXCLUDED.pf,
                rdfid = EXCLUDED.rdfid,
                action = EXCLUDED.action,
                comment = EXCLUDED.comment,
                modelling_authority = EXCLUDED.modelling_authority,
                commissioned_date = EXCLUDED.commissioned_date,
                decommissioned_date = EXCLUDED.decommissioned_date,
                active_prof = EXCLUDED.active_prof,
                mttf = EXCLUDED.mttf,
                mttr = EXCLUDED.mttr,
                capex = EXCLUDED.capex,
                opex = EXCLUDED.opex,
                build_status = EXCLUDED.build_status,
                cost = EXCLUDED.cost,
                cost_prof = EXCLUDED.cost_prof,
                facility = EXCLUDED.facility,
                technologies = EXCLUDED.technologies,
                scalable = EXCLUDED.scalable,
                shift_key = EXCLUDED.shift_key,
                shift_key_prof = EXCLUDED.shift_key_prof,
                longitude = EXCLUDED.longitude,
                latitude = EXCLUDED.latitude,
                use_kw = EXCLUDED.use_kw,
                conn = EXCLUDED.conn,
                rms_model = EXCLUDED.rms_model,
                bus_pos = EXCLUDED.bus_pos,
                control_bus = EXCLUDED.control_bus,
                control_bus_prof = EXCLUDED.control_bus_prof,
                p_prof = EXCLUDED.p_prof,
                pmin = EXCLUDED.pmin,
                pmin_prof = EXCLUDED.pmin_prof,
                pmax = EXCLUDED.pmax,
                pmax_prof = EXCLUDED.pmax_prof,
                srap_enabled = EXCLUDED.srap_enabled,
                srap_enabled_prof = EXCLUDED.srap_enabled_prof,
                is_controlled = EXCLUDED.is_controlled,
                pf_prof = EXCLUDED.pf_prof,
                vset_prof = EXCLUDED.vset_prof,
                snom = EXCLUDED.snom,
                qmin_prof = EXCLUDED.qmin_prof,
                qmax_prof = EXCLUDED.qmax_prof,
                use_reactive_power_curve = EXCLUDED.use_reactive_power_curve,
                q_curve = EXCLUDED.q_curve,
                r1 = EXCLUDED.r1,
                x1 = EXCLUDED.x1,
                r0 = EXCLUDED.r0,
                x0 = EXCLUDED.x0,
                r2 = EXCLUDED.r2,
                x2 = EXCLUDED.x2,
                cost2 = EXCLUDED.cost2,
                cost2_prof = EXCLUDED.cost2_prof,
                cost0 = EXCLUDED.cost0,
                cost0_prof = EXCLUDED.cost0_prof,
                startupcost = EXCLUDED.startupcost,
                shutdowncost = EXCLUDED.shutdowncost,
                mintimeup = EXCLUDED.mintimeup,
                mintimedown = EXCLUDED.mintimedown,
                rampup = EXCLUDED.rampup,
                rampdown = EXCLUDED.rampdown,
                enabled_dispatch = EXCLUDED.enabled_dispatch,
                emissions = EXCLUDED.emissions,
                fuels = EXCLUDED.fuels
            """,
            rows,
        )
    return len(rows)


def get_generator_by_id(generator_id: int):
    """Get a generator by its internal ID."""
    conn = get_conn()
    ensure_schema(conn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, grid_id, idtag, name, code, bus_idtag, active, p, vset, qmin, qmax, pf,
                       rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
                       active_prof, mttf, mttr, capex, opex, build_status, cost, cost_prof, facility,
                       technologies, scalable, shift_key, shift_key_prof, longitude, latitude, use_kw,
                       conn, rms_model, bus_pos, control_bus, control_bus_prof, p_prof, pmin, pmin_prof,
                       pmax, pmax_prof, srap_enabled, srap_enabled_prof, is_controlled, pf_prof, vset_prof,
                       snom, qmin_prof, qmax_prof, use_reactive_power_curve, q_curve, r1, x1, r0, x0, r2, x2,
                       cost2, cost2_prof, cost0, cost0_prof, startupcost, shutdowncost, mintimeup, mintimedown,
                       rampup, rampdown, enabled_dispatch, emissions, fuels
                FROM generators WHERE id = %s;
                """,
                (generator_id,),
            )
            return cur.fetchone()
    finally:
        conn.close()


def list_generators():
    """Return all generators ordered by id."""
    conn = get_conn()
    ensure_schema(conn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, grid_id, idtag, name, code, bus_idtag, active, p, vset, qmin, qmax, pf,
                       rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
                       active_prof, mttf, mttr, capex, opex, build_status, cost, cost_prof, facility,
                       technologies, scalable, shift_key, shift_key_prof, longitude, latitude, use_kw,
                       conn, rms_model, bus_pos, control_bus, control_bus_prof, p_prof, pmin, pmin_prof,
                       pmax, pmax_prof, srap_enabled, srap_enabled_prof, is_controlled, pf_prof, vset_prof,
                       snom, qmin_prof, qmax_prof, use_reactive_power_curve, q_curve, r1, x1, r0, x0, r2, x2,
                       cost2, cost2_prof, cost0, cost0_prof, startupcost, shutdowncost, mintimeup, mintimedown,
                       rampup, rampdown, enabled_dispatch, emissions, fuels
                FROM generators
                ORDER BY id;
                """
            )
            return cur.fetchall()
    finally:
        conn.close()


def update_generator_status(generator_id: int, active: bool):
    """Update the active status of a generator."""
    conn = get_conn()
    ensure_schema(conn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE generators SET active = %s WHERE id = %s
                RETURNING id, grid_id, idtag;
                """,
                (active, generator_id),
            )
            result = cur.fetchone()
            conn.commit()
            return result
    finally:
        conn.close()
