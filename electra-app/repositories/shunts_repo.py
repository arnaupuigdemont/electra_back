from psycopg2.extras import execute_values, Json
from db.db import get_conn


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS shunts (
                id SERIAL PRIMARY KEY,
                grid_id INTEGER NOT NULL REFERENCES grids(id) ON DELETE CASCADE,
                idtag TEXT NOT NULL,
                name TEXT,
                code TEXT,
                bus_idtag TEXT NOT NULL,
                active BOOLEAN,
                b DOUBLE PRECISION,
                UNIQUE (grid_id, idtag),
                FOREIGN KEY (grid_id, bus_idtag) REFERENCES buses(grid_id, idtag) ON DELETE CASCADE
            );
            """
        )
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS grid_id INTEGER;")
        cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                WHERE c.conname = 'fk_shunts_grid'
                  AND t.relname = 'shunts'
            ) THEN
                ALTER TABLE shunts
                ADD CONSTRAINT fk_shunts_grid
                FOREIGN KEY (grid_id) REFERENCES grids(id) ON DELETE CASCADE;
            END IF;
        END$$;
        """)
        
        # Add extended fields
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS rdfid TEXT;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS action TEXT;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS comment TEXT;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS modelling_authority TEXT;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS commissioned_date BIGINT;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS decommissioned_date BIGINT;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS active_prof JSONB;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS mttf DOUBLE PRECISION;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS mttr DOUBLE PRECISION;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS capex DOUBLE PRECISION;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS opex DOUBLE PRECISION;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS build_status TEXT;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS cost DOUBLE PRECISION;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS cost_prof JSONB;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS facility TEXT;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS technologies JSONB;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS scalable BOOLEAN;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS shift_key DOUBLE PRECISION;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS shift_key_prof JSONB;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS use_kw BOOLEAN;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS conn TEXT;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS rms_model JSONB;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS bus_pos INTEGER;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS g DOUBLE PRECISION;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS g_prof JSONB;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS g0 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS g0_prof JSONB;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS ga DOUBLE PRECISION;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS ga_prof JSONB;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS gb DOUBLE PRECISION;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS gb_prof JSONB;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS gc DOUBLE PRECISION;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS gc_prof JSONB;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS b_prof JSONB;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS b0 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS b0_prof JSONB;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS ba DOUBLE PRECISION;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS ba_prof JSONB;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS bb DOUBLE PRECISION;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS bb_prof JSONB;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS bc DOUBLE PRECISION;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS bc_prof JSONB;")
        cur.execute("ALTER TABLE shunts ADD COLUMN IF NOT EXISTS ysh JSONB;")
    conn.commit()


def upsert(conn, grid_id: int, shunts: list[dict]) -> int:
    def adapt(val):
        """Wrap dict/list in Json() to ensure proper JSONB adaptation."""
        if isinstance(val, (dict, list)):
            return Json(val)
        return val
    
    rows = [
        (
            grid_id,
            s.get("idtag"),
            s.get("name"),
            s.get("code"),
            s.get("bus"),
            bool(s.get("active", True)),
            s.get("B"),
            s.get("rdfid"),
            s.get("action"),
            s.get("comment"),
            s.get("modelling_authority"),
            s.get("commissioned_date"),
            s.get("decommissioned_date"),
            adapt(s.get("active_prof")),
            s.get("mttf"),
            s.get("mttr"),
            s.get("capex"),
            s.get("opex"),
            s.get("build_status"),
            s.get("Cost"),
            adapt(s.get("Cost_prof")),
            s.get("facility"),
            adapt(s.get("technologies")),
            s.get("scalable"),
            s.get("shift_key"),
            adapt(s.get("shift_key_prof")),
            s.get("longitude"),
            s.get("latitude"),
            s.get("use_kw"),
            s.get("conn"),
            adapt(s.get("rms_model")),
            s.get("bus_pos"),
            s.get("G"),
            adapt(s.get("G_prof")),
            s.get("G0"),
            adapt(s.get("G0_prof")),
            s.get("Ga"),
            adapt(s.get("Ga_prof")),
            s.get("Gb"),
            adapt(s.get("Gb_prof")),
            s.get("Gc"),
            adapt(s.get("Gc_prof")),
            adapt(s.get("B_prof")),
            s.get("B0"),
            adapt(s.get("B0_prof")),
            s.get("Ba"),
            adapt(s.get("Ba_prof")),
            s.get("Bb"),
            adapt(s.get("Bb_prof")),
            s.get("Bc"),
            adapt(s.get("Bc_prof")),
            adapt(s.get("ysh")),
        )
        for s in (shunts or [])
        if s.get("idtag") and s.get("bus")
    ]
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO shunts (
                grid_id, idtag, name, code, bus_idtag, active, b,
                rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
                active_prof, mttf, mttr, capex, opex, build_status, cost, cost_prof, facility,
                technologies, scalable, shift_key, shift_key_prof, longitude, latitude, use_kw,
                conn, rms_model, bus_pos, g, g_prof, g0, g0_prof, ga, ga_prof, gb, gb_prof,
                gc, gc_prof, b_prof, b0, b0_prof, ba, ba_prof, bb, bb_prof, bc, bc_prof, ysh
            ) VALUES %s
            ON CONFLICT (grid_id, idtag) DO UPDATE SET
                name = EXCLUDED.name,
                code = EXCLUDED.code,
                bus_idtag = EXCLUDED.bus_idtag,
                active = EXCLUDED.active,
                b = EXCLUDED.b,
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
                g = EXCLUDED.g,
                g_prof = EXCLUDED.g_prof,
                g0 = EXCLUDED.g0,
                g0_prof = EXCLUDED.g0_prof,
                ga = EXCLUDED.ga,
                ga_prof = EXCLUDED.ga_prof,
                gb = EXCLUDED.gb,
                gb_prof = EXCLUDED.gb_prof,
                gc = EXCLUDED.gc,
                gc_prof = EXCLUDED.gc_prof,
                b_prof = EXCLUDED.b_prof,
                b0 = EXCLUDED.b0,
                b0_prof = EXCLUDED.b0_prof,
                ba = EXCLUDED.ba,
                ba_prof = EXCLUDED.ba_prof,
                bb = EXCLUDED.bb,
                bb_prof = EXCLUDED.bb_prof,
                bc = EXCLUDED.bc,
                bc_prof = EXCLUDED.bc_prof,
                ysh = EXCLUDED.ysh
            """,
            rows,
        )
    return len(rows)


def get_shunt_by_id(shunt_id: int):
    """Return shunt by its internal ID."""
    conn = get_conn()
    ensure_schema(conn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, grid_id, idtag, name, code, bus_idtag, active, b,
                       rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
                       active_prof, mttf, mttr, capex, opex, build_status, cost, cost_prof, facility,
                       technologies, scalable, shift_key, shift_key_prof, longitude, latitude, use_kw,
                       conn, rms_model, bus_pos, g, g_prof, g0, g0_prof, ga, ga_prof, gb, gb_prof,
                       gc, gc_prof, b_prof, b0, b0_prof, ba, ba_prof, bb, bb_prof, bc, bc_prof, ysh
                FROM shunts WHERE id = %s;
                """,
                (shunt_id,),
            )
            return cur.fetchone()
    finally:
        conn.close()


def list_shunts():
    """Return all shunts ordered by id."""
    conn = get_conn()
    ensure_schema(conn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, grid_id, idtag, name, code, bus_idtag, active, b,
                       rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
                       active_prof, mttf, mttr, capex, opex, build_status, cost, cost_prof, facility,
                       technologies, scalable, shift_key, shift_key_prof, longitude, latitude, use_kw,
                       conn, rms_model, bus_pos, g, g_prof, g0, g0_prof, ga, ga_prof, gb, gb_prof,
                       gc, gc_prof, b_prof, b0, b0_prof, ba, ba_prof, bb, bb_prof, bc, bc_prof, ysh
                FROM shunts
                ORDER BY id;
                """
            )
            return cur.fetchall()
    finally:
        conn.close()


def update_shunt_status(shunt_id: int, active: bool):
    """Update the active status of a shunt."""
    conn = get_conn()
    ensure_schema(conn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE shunts SET active = %s WHERE id = %s
                RETURNING id, grid_id, idtag;
                """,
                (active, shunt_id),
            )
            result = cur.fetchone()
            conn.commit()
            return result
    finally:
        conn.close()
