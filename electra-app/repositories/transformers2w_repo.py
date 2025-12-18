from psycopg2.extras import execute_values, Json
from db.db import get_conn


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS transformers2w (
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
                g DOUBLE PRECISION,
                b DOUBLE PRECISION,
                hv DOUBLE PRECISION,
                lv DOUBLE PRECISION,
                sn DOUBLE PRECISION,
                UNIQUE (grid_id, idtag),
                FOREIGN KEY (grid_id, bus_from_idtag) REFERENCES buses(grid_id, idtag) ON DELETE CASCADE,
                FOREIGN KEY (grid_id, bus_to_idtag) REFERENCES buses(grid_id, idtag) ON DELETE CASCADE
            );
            """
        )
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS grid_id INTEGER;")
        cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                WHERE c.conname = 'fk_transformers2w_grid'
                  AND t.relname = 'transformers2w'
            ) THEN
                ALTER TABLE transformers2w
                ADD CONSTRAINT fk_transformers2w_grid
                FOREIGN KEY (grid_id) REFERENCES grids(id) ON DELETE CASCADE;
            END IF;
        END$$;
        """)
        
        # Add comprehensive transformer fields
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS rdfid TEXT;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS action TEXT;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS comment TEXT;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS modelling_authority TEXT;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS commissioned_date DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS decommissioned_date DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS active_prof JSONB;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS reducible BOOLEAN;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS rate DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS rate_prof JSONB;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS contingency_factor DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS contingency_factor_prof JSONB;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS protection_rating_factor DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS protection_rating_factor_prof JSONB;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS monitor_loading BOOLEAN;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS mttf DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS mttr DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS cost DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS cost_prof JSONB;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS build_status TEXT;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS capex DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS opex DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS tx_group TEXT;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS color TEXT;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS rms_model JSONB;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS bus_from_pos DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS bus_to_pos DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS r0 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS x0 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS g0 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS b0 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS r2 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS x2 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS g2 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS b2 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS tolerance DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS tap_changer JSONB;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS tap_module DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS tap_module_prof JSONB;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS tap_module_max DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS tap_module_min DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS tap_module_control_mode TEXT;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS tap_module_control_mode_prof JSONB;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS vset DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS vset_prof JSONB;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS qset DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS qset_prof JSONB;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS regulation_bus TEXT;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS tap_phase DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS tap_phase_prof JSONB;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS tap_phase_max DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS tap_phase_min DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS tap_phase_control_mode TEXT;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS tap_phase_control_mode_prof JSONB;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS pset DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS pset_prof JSONB;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS temp_base DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS temp_oper DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS temp_oper_prof JSONB;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS alpha DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS pcu DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS pfe DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS i0 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS vsc DOUBLE PRECISION;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS conn TEXT;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS conn_f TEXT;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS conn_t TEXT;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS vector_group_number INTEGER;")
        cur.execute("ALTER TABLE transformers2w ADD COLUMN IF NOT EXISTS template TEXT;")
    conn.commit()


def upsert(conn, grid_id: int, transformers: list[dict]) -> int:
    def adapt(val):
        """Wrap dict/list in Json() for JSONB columns."""
        if isinstance(val, (dict, list)):
            return Json(val)
        return val
    
    rows = [
        (
            grid_id,
            t.get("idtag"),
            t.get("name"),
            t.get("code"),
            t.get("bus_from"),
            t.get("bus_to"),
            bool(t.get("active", True)),
            t.get("R"),
            t.get("X"),
            t.get("G"),
            t.get("B"),
            t.get("HV"),
            t.get("LV"),
            t.get("Sn"),
            t.get("rdfid"),
            t.get("action"),
            t.get("comment"),
            t.get("modelling_authority"),
            t.get("commissioned_date"),
            t.get("decommissioned_date"),
            adapt(t.get("active_prof")),
            t.get("reducible"),
            t.get("rate"),
            adapt(t.get("rate_prof")),
            t.get("contingency_factor"),
            adapt(t.get("contingency_factor_prof")),
            t.get("protection_rating_factor"),
            adapt(t.get("protection_rating_factor_prof")),
            t.get("monitor_loading"),
            t.get("mttf"),
            t.get("mttr"),
            t.get("Cost"),
            adapt(t.get("Cost_prof")),
            t.get("build_status"),
            t.get("capex"),
            t.get("opex"),
            t.get("group"),
            t.get("color"),
            adapt(t.get("rms_model")),
            t.get("bus_from_pos"),
            t.get("bus_to_pos"),
            t.get("R0"),
            t.get("X0"),
            t.get("G0"),
            t.get("B0"),
            t.get("R2"),
            t.get("X2"),
            t.get("G2"),
            t.get("B2"),
            t.get("tolerance"),
            adapt(t.get("tap_changer")),
            t.get("tap_module"),
            adapt(t.get("tap_module_prof")),
            t.get("tap_module_max"),
            t.get("tap_module_min"),
            t.get("tap_module_control_mode"),
            adapt(t.get("tap_module_control_mode_prof")),
            t.get("vset"),
            adapt(t.get("vset_prof")),
            t.get("Qset"),
            adapt(t.get("Qset_prof")),
            t.get("regulation_bus"),
            t.get("tap_phase"),
            adapt(t.get("tap_phase_prof")),
            t.get("tap_phase_max"),
            t.get("tap_phase_min"),
            t.get("tap_phase_control_mode"),
            adapt(t.get("tap_phase_control_mode_prof")),
            t.get("Pset"),
            adapt(t.get("Pset_prof")),
            t.get("temp_base"),
            t.get("temp_oper"),
            adapt(t.get("temp_oper_prof")),
            t.get("alpha"),
            t.get("Pcu"),
            t.get("Pfe"),
            t.get("I0"),
            t.get("Vsc"),
            t.get("conn"),
            t.get("conn_f"),
            t.get("conn_t"),
            t.get("vector_group_number"),
            t.get("template"),
        )
        for t in (transformers or [])
        if t.get("idtag") and t.get("bus_from") and t.get("bus_to")
    ]
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO transformers2w (
                grid_id, idtag, name, code, bus_from_idtag, bus_to_idtag, active,
                r, x, g, b, hv, lv, sn,
                rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
                active_prof, reducible, rate, rate_prof, contingency_factor, contingency_factor_prof,
                protection_rating_factor, protection_rating_factor_prof, monitor_loading, mttf, mttr,
                cost, cost_prof, build_status, capex, opex, tx_group, color, rms_model,
                bus_from_pos, bus_to_pos, r0, x0, g0, b0, r2, x2, g2, b2, tolerance,
                tap_changer, tap_module, tap_module_prof, tap_module_max, tap_module_min,
                tap_module_control_mode, tap_module_control_mode_prof, vset, vset_prof,
                qset, qset_prof, regulation_bus, tap_phase, tap_phase_prof, tap_phase_max,
                tap_phase_min, tap_phase_control_mode, tap_phase_control_mode_prof, pset,
                pset_prof, temp_base, temp_oper, temp_oper_prof, alpha, pcu, pfe, i0, vsc,
                conn, conn_f, conn_t, vector_group_number, template
            ) VALUES %s
            ON CONFLICT (grid_id, idtag) DO UPDATE SET
                name = EXCLUDED.name,
                code = EXCLUDED.code,
                bus_from_idtag = EXCLUDED.bus_from_idtag,
                bus_to_idtag = EXCLUDED.bus_to_idtag,
                active = EXCLUDED.active,
                r = EXCLUDED.r,
                x = EXCLUDED.x,
                g = EXCLUDED.g,
                b = EXCLUDED.b,
                hv = EXCLUDED.hv,
                lv = EXCLUDED.lv,
                sn = EXCLUDED.sn,
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
                tx_group = EXCLUDED.tx_group,
                color = EXCLUDED.color,
                rms_model = EXCLUDED.rms_model,
                bus_from_pos = EXCLUDED.bus_from_pos,
                bus_to_pos = EXCLUDED.bus_to_pos,
                r0 = EXCLUDED.r0,
                x0 = EXCLUDED.x0,
                g0 = EXCLUDED.g0,
                b0 = EXCLUDED.b0,
                r2 = EXCLUDED.r2,
                x2 = EXCLUDED.x2,
                g2 = EXCLUDED.g2,
                b2 = EXCLUDED.b2,
                tolerance = EXCLUDED.tolerance,
                tap_changer = EXCLUDED.tap_changer,
                tap_module = EXCLUDED.tap_module,
                tap_module_prof = EXCLUDED.tap_module_prof,
                tap_module_max = EXCLUDED.tap_module_max,
                tap_module_min = EXCLUDED.tap_module_min,
                tap_module_control_mode = EXCLUDED.tap_module_control_mode,
                tap_module_control_mode_prof = EXCLUDED.tap_module_control_mode_prof,
                vset = EXCLUDED.vset,
                vset_prof = EXCLUDED.vset_prof,
                qset = EXCLUDED.qset,
                qset_prof = EXCLUDED.qset_prof,
                regulation_bus = EXCLUDED.regulation_bus,
                tap_phase = EXCLUDED.tap_phase,
                tap_phase_prof = EXCLUDED.tap_phase_prof,
                tap_phase_max = EXCLUDED.tap_phase_max,
                tap_phase_min = EXCLUDED.tap_phase_min,
                tap_phase_control_mode = EXCLUDED.tap_phase_control_mode,
                tap_phase_control_mode_prof = EXCLUDED.tap_phase_control_mode_prof,
                pset = EXCLUDED.pset,
                pset_prof = EXCLUDED.pset_prof,
                temp_base = EXCLUDED.temp_base,
                temp_oper = EXCLUDED.temp_oper,
                temp_oper_prof = EXCLUDED.temp_oper_prof,
                alpha = EXCLUDED.alpha,
                pcu = EXCLUDED.pcu,
                pfe = EXCLUDED.pfe,
                i0 = EXCLUDED.i0,
                vsc = EXCLUDED.vsc,
                conn = EXCLUDED.conn,
                conn_f = EXCLUDED.conn_f,
                conn_t = EXCLUDED.conn_t,
                vector_group_number = EXCLUDED.vector_group_number,
                template = EXCLUDED.template
            """,
            rows,
        )
    return len(rows)


def get_transformer2w_by_id(transformer_id: int):
    """Return 2-winding transformer by its internal ID."""
    conn = get_conn()
    ensure_schema(conn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, grid_id, idtag, name, code, bus_from_idtag, bus_to_idtag, active,
                       r, x, g, b, hv, lv, sn,
                       rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
                       active_prof, reducible, rate, rate_prof, contingency_factor, contingency_factor_prof,
                       protection_rating_factor, protection_rating_factor_prof, monitor_loading, mttf, mttr,
                       cost, cost_prof, build_status, capex, opex, tx_group, color, rms_model,
                       bus_from_pos, bus_to_pos, r0, x0, g0, b0, r2, x2, g2, b2, tolerance,
                       tap_changer, tap_module, tap_module_prof, tap_module_max, tap_module_min,
                       tap_module_control_mode, tap_module_control_mode_prof, vset, vset_prof,
                       qset, qset_prof, regulation_bus, tap_phase, tap_phase_prof, tap_phase_max,
                       tap_phase_min, tap_phase_control_mode, tap_phase_control_mode_prof, pset,
                       pset_prof, temp_base, temp_oper, temp_oper_prof, alpha, pcu, pfe, i0, vsc,
                       conn, conn_f, conn_t, vector_group_number, template
                FROM transformers2w WHERE id = %s;
                """,
                (transformer_id,),
            )
            return cur.fetchone()
    finally:
        conn.close()


def list_transformers2w():
    """Return all 2-winding transformers ordered by id."""
    conn = get_conn()
    ensure_schema(conn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, grid_id, idtag, name, code, bus_from_idtag, bus_to_idtag, active,
                       r, x, g, b, hv, lv, sn,
                       rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
                       active_prof, reducible, rate, rate_prof, contingency_factor, contingency_factor_prof,
                       protection_rating_factor, protection_rating_factor_prof, monitor_loading, mttf, mttr,
                       cost, cost_prof, build_status, capex, opex, tx_group, color, rms_model,
                       bus_from_pos, bus_to_pos, r0, x0, g0, b0, r2, x2, g2, b2, tolerance,
                       tap_changer, tap_module, tap_module_prof, tap_module_max, tap_module_min,
                       tap_module_control_mode, tap_module_control_mode_prof, vset, vset_prof,
                       qset, qset_prof, regulation_bus, tap_phase, tap_phase_prof, tap_phase_max,
                       tap_phase_min, tap_phase_control_mode, tap_phase_control_mode_prof, pset,
                       pset_prof, temp_base, temp_oper, temp_oper_prof, alpha, pcu, pfe, i0, vsc,
                       conn, conn_f, conn_t, vector_group_number, template
                FROM transformers2w
                ORDER BY id;
                """
            )
            return cur.fetchall()
    finally:
        conn.close()


def update_transformer_status(transformer_id: int, active: bool):
    """Update the active status of a transformer."""
    conn = get_conn()
    ensure_schema(conn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE transformers2w SET active = %s WHERE id = %s
                RETURNING id, grid_id, idtag;
                """,
                (active, transformer_id),
            )
            result = cur.fetchone()
            conn.commit()
            return result
    finally:
        conn.close()
