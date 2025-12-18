from psycopg2.extras import execute_values, Json
from db.db import get_conn


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS lines (
                id SERIAL PRIMARY KEY,
                grid_id INTEGER NOT NULL REFERENCES grids(id) ON DELETE CASCADE,
                idtag TEXT NOT NULL,
                name TEXT,
                code TEXT,
                bus_from_idtag TEXT NOT NULL,
                bus_to_idtag TEXT NOT NULL,
                active BOOLEAN,
                r DOUBLE PRECISION,
                x DOUBLE PRECISION,
                b DOUBLE PRECISION,
                length DOUBLE PRECISION,
                UNIQUE (grid_id, idtag),
                FOREIGN KEY (grid_id, bus_from_idtag) REFERENCES buses(grid_id, idtag) ON DELETE CASCADE,
                FOREIGN KEY (grid_id, bus_to_idtag) REFERENCES buses(grid_id, idtag) ON DELETE CASCADE
            );
            """
        )
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS grid_id INTEGER;")
        cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                WHERE c.conname = 'fk_lines_grid'
                  AND t.relname = 'lines'
            ) THEN
                ALTER TABLE lines
                ADD CONSTRAINT fk_lines_grid
                FOREIGN KEY (grid_id) REFERENCES grids(id) ON DELETE CASCADE;
            END IF;
        END$$;
        """)
        
        # Add extended fields
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS rdfid TEXT;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS action TEXT;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS comment TEXT;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS modelling_authority TEXT;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS commissioned_date BIGINT;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS decommissioned_date BIGINT;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS active_prof JSONB;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS reducible BOOLEAN;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS rate DOUBLE PRECISION;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS rate_prof JSONB;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS contingency_factor DOUBLE PRECISION;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS contingency_factor_prof JSONB;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS protection_rating_factor DOUBLE PRECISION;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS protection_rating_factor_prof JSONB;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS monitor_loading BOOLEAN;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS mttf DOUBLE PRECISION;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS mttr DOUBLE PRECISION;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS cost DOUBLE PRECISION;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS cost_prof JSONB;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS build_status TEXT;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS capex DOUBLE PRECISION;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS opex DOUBLE PRECISION;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS line_group TEXT;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS color TEXT;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS rms_model JSONB;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS bus_from_pos INTEGER;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS bus_to_pos INTEGER;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS r0 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS x0 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS b0 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS r2 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS x2 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS b2 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS ys JSONB;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS ysh JSONB;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS tolerance DOUBLE PRECISION;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS circuit_idx INTEGER;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS temp_base DOUBLE PRECISION;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS temp_oper DOUBLE PRECISION;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS temp_oper_prof JSONB;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS alpha DOUBLE PRECISION;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS r_fault DOUBLE PRECISION;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS x_fault DOUBLE PRECISION;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS fault_pos DOUBLE PRECISION;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS template TEXT;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS locations JSONB;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS possible_tower_types JSONB;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS possible_underground_line_types JSONB;")
        cur.execute("ALTER TABLE lines ADD COLUMN IF NOT EXISTS possible_sequence_line_types JSONB;")
    conn.commit()


def upsert(conn, grid_id: int, lines: list[dict]) -> int:
    def adapt(val):
        """Wrap dict/list in Json() to ensure proper JSONB adaptation."""
        if isinstance(val, (dict, list)):
            return Json(val)
        return val
    
    rows = [
        (
            grid_id,
            ln.get("idtag"),
            ln.get("name"),
            ln.get("code"),
            ln.get("bus_from"),
            ln.get("bus_to"),
            bool(ln.get("active", True)),
            ln.get("R"),
            ln.get("X"),
            ln.get("B"),
            ln.get("length"),
            ln.get("rdfid"),
            ln.get("action"),
            ln.get("comment"),
            ln.get("modelling_authority"),
            ln.get("commissioned_date"),
            ln.get("decommissioned_date"),
            adapt(ln.get("active_prof")),
            ln.get("reducible"),
            ln.get("rate"),
            adapt(ln.get("rate_prof")),
            ln.get("contingency_factor"),
            adapt(ln.get("contingency_factor_prof")),
            ln.get("protection_rating_factor"),
            adapt(ln.get("protection_rating_factor_prof")),
            ln.get("monitor_loading"),
            ln.get("mttf"),
            ln.get("mttr"),
            ln.get("Cost"),
            adapt(ln.get("Cost_prof")),
            ln.get("build_status"),
            ln.get("capex"),
            ln.get("opex"),
            ln.get("group"),
            ln.get("color"),
            adapt(ln.get("rms_model")),
            ln.get("bus_from_pos"),
            ln.get("bus_to_pos"),
            ln.get("R0"),
            ln.get("X0"),
            ln.get("B0"),
            ln.get("R2"),
            ln.get("X2"),
            ln.get("B2"),
            adapt(ln.get("ys")),
            adapt(ln.get("ysh")),
            ln.get("tolerance"),
            ln.get("circuit_idx"),
            ln.get("temp_base"),
            ln.get("temp_oper"),
            adapt(ln.get("temp_oper_prof")),
            ln.get("alpha"),
            ln.get("r_fault"),
            ln.get("x_fault"),
            ln.get("fault_pos"),
            ln.get("template"),
            adapt(ln.get("locations")),
            adapt(ln.get("possible_tower_types")),
            adapt(ln.get("possible_underground_line_types")),
            adapt(ln.get("possible_sequence_line_types")),
        )
        for ln in (lines or [])
        if ln.get("idtag") and ln.get("bus_from") and ln.get("bus_to")
    ]
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO lines (
                grid_id, idtag, name, code, bus_from_idtag, bus_to_idtag, active,
                r, x, b, length,
                rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
                active_prof, reducible, rate, rate_prof, contingency_factor, contingency_factor_prof,
                protection_rating_factor, protection_rating_factor_prof, monitor_loading, mttf, mttr,
                cost, cost_prof, build_status, capex, opex, line_group, color, rms_model,
                bus_from_pos, bus_to_pos, r0, x0, b0, r2, x2, b2, ys, ysh, tolerance, circuit_idx,
                temp_base, temp_oper, temp_oper_prof, alpha, r_fault, x_fault, fault_pos, template,
                locations, possible_tower_types, possible_underground_line_types, possible_sequence_line_types
            ) VALUES %s
            ON CONFLICT (grid_id, idtag) DO UPDATE SET
                name = EXCLUDED.name,
                code = EXCLUDED.code,
                bus_from_idtag = EXCLUDED.bus_from_idtag,
                bus_to_idtag = EXCLUDED.bus_to_idtag,
                active = EXCLUDED.active,
                r = EXCLUDED.r,
                x = EXCLUDED.x,
                b = EXCLUDED.b,
                length = EXCLUDED.length,
                rdfid = EXCLUDED.rdfid,
                action = EXCLUDED.action,
                comment = EXCLUDED.comment,
                modelling_authority = EXCLUDED.modelling_authority,
                commissioned_date = EXCLUDED.commissioned_date,
                decommissioned_date = EXCLUDED.decommissioned_date,
                active_prof = EXCLUDED.active_prof,
                reducible = EXCLUDED.reducible,
                rate = EXCLUDED.rate,
                rate_prof = EXCLUDED.rate_prof,
                contingency_factor = EXCLUDED.contingency_factor,
                contingency_factor_prof = EXCLUDED.contingency_factor_prof,
                protection_rating_factor = EXCLUDED.protection_rating_factor,
                protection_rating_factor_prof = EXCLUDED.protection_rating_factor_prof,
                monitor_loading = EXCLUDED.monitor_loading,
                mttf = EXCLUDED.mttf,
                mttr = EXCLUDED.mttr,
                cost = EXCLUDED.cost,
                cost_prof = EXCLUDED.cost_prof,
                build_status = EXCLUDED.build_status,
                capex = EXCLUDED.capex,
                opex = EXCLUDED.opex,
                line_group = EXCLUDED.line_group,
                color = EXCLUDED.color,
                rms_model = EXCLUDED.rms_model,
                bus_from_pos = EXCLUDED.bus_from_pos,
                bus_to_pos = EXCLUDED.bus_to_pos,
                r0 = EXCLUDED.r0,
                x0 = EXCLUDED.x0,
                b0 = EXCLUDED.b0,
                r2 = EXCLUDED.r2,
                x2 = EXCLUDED.x2,
                b2 = EXCLUDED.b2,
                ys = EXCLUDED.ys,
                ysh = EXCLUDED.ysh,
                tolerance = EXCLUDED.tolerance,
                circuit_idx = EXCLUDED.circuit_idx,
                temp_base = EXCLUDED.temp_base,
                temp_oper = EXCLUDED.temp_oper,
                temp_oper_prof = EXCLUDED.temp_oper_prof,
                alpha = EXCLUDED.alpha,
                r_fault = EXCLUDED.r_fault,
                x_fault = EXCLUDED.x_fault,
                fault_pos = EXCLUDED.fault_pos,
                template = EXCLUDED.template,
                locations = EXCLUDED.locations,
                possible_tower_types = EXCLUDED.possible_tower_types,
                possible_underground_line_types = EXCLUDED.possible_underground_line_types,
                possible_sequence_line_types = EXCLUDED.possible_sequence_line_types
            """,
            rows,
        )
    return len(rows)


def get_line_by_id(line_id: int):
    """Return line by its internal ID."""
    conn = get_conn()
    ensure_schema(conn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, grid_id, idtag, name, code, bus_from_idtag, bus_to_idtag, active,
                       r, x, b, length,
                       rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
                       active_prof, reducible, rate, rate_prof, contingency_factor, contingency_factor_prof,
                       protection_rating_factor, protection_rating_factor_prof, monitor_loading, mttf, mttr,
                       cost, cost_prof, build_status, capex, opex, line_group, color, rms_model,
                       bus_from_pos, bus_to_pos, r0, x0, b0, r2, x2, b2, ys, ysh, tolerance, circuit_idx,
                       temp_base, temp_oper, temp_oper_prof, alpha, r_fault, x_fault, fault_pos, template,
                       locations, possible_tower_types, possible_underground_line_types, possible_sequence_line_types
                FROM lines WHERE id = %s;
                """,
                (line_id,),
            )
            return cur.fetchone()
    finally:
        conn.close()


def list_lines():
    """Return all lines ordered by id."""
    conn = get_conn()
    ensure_schema(conn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, grid_id, idtag, name, code, bus_from_idtag, bus_to_idtag, active,
                       r, x, b, length,
                       rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
                       active_prof, reducible, rate, rate_prof, contingency_factor, contingency_factor_prof,
                       protection_rating_factor, protection_rating_factor_prof, monitor_loading, mttf, mttr,
                       cost, cost_prof, build_status, capex, opex, line_group, color, rms_model,
                       bus_from_pos, bus_to_pos, r0, x0, b0, r2, x2, b2, ys, ysh, tolerance, circuit_idx,
                       temp_base, temp_oper, temp_oper_prof, alpha, r_fault, x_fault, fault_pos, template,
                       locations, possible_tower_types, possible_underground_line_types, possible_sequence_line_types
                FROM lines
                ORDER BY id;
                """
            )
            return cur.fetchall()
    finally:
        conn.close()


def update_line_status(line_id: int, active: bool):
    """Update the active status of a line."""
    conn = get_conn()
    ensure_schema(conn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE lines SET active = %s WHERE id = %s
                RETURNING id, grid_id, idtag;
                """,
                (active, line_id),
            )
            result = cur.fetchone()
            conn.commit()
            return result
    finally:
        conn.close()
