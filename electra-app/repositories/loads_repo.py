from psycopg2.extras import execute_values, Json
from db.db import get_conn


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS loads (
                id SERIAL PRIMARY KEY,
                grid_id INTEGER NOT NULL REFERENCES grids(id) ON DELETE CASCADE,
                idtag TEXT NOT NULL,
                name TEXT,
                code TEXT,
                bus_idtag TEXT NOT NULL,
                active BOOLEAN,
                p DOUBLE PRECISION,
                q DOUBLE PRECISION,
                conn TEXT,
                longitude DOUBLE PRECISION,
                latitude DOUBLE PRECISION,
                UNIQUE (grid_id, idtag),
                FOREIGN KEY (grid_id, bus_idtag) REFERENCES buses(grid_id, idtag) ON DELETE CASCADE
            );
            """
        )
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS grid_id INTEGER;")
        cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint c
                JOIN pg_class t ON t.oid = c.conrelid
                WHERE c.conname = 'fk_loads_grid'
                  AND t.relname = 'loads'
            ) THEN
                ALTER TABLE loads
                ADD CONSTRAINT fk_loads_grid
                FOREIGN KEY (grid_id) REFERENCES grids(id) ON DELETE CASCADE;
            END IF;
        END$$;
        """)
        
        # Add extended fields
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS rdfid TEXT;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS action TEXT;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS comment TEXT;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS modelling_authority TEXT;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS commissioned_date BIGINT;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS decommissioned_date BIGINT;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS active_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS mttf DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS mttr DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS capex DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS opex DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS build_status TEXT;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS cost DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS cost_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS facility TEXT;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS technologies JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS scalable BOOLEAN;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS shift_key DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS shift_key_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS use_kw BOOLEAN;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS rms_model JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS bus_pos INTEGER;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS p_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS pa DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS pa_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS pb DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS pb_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS pc DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS pc_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS q_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS qa DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS qa_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS qb DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS qb_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS qc DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS qc_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS ir DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS ir_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS ir1 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS ir1_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS ir2 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS ir2_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS ir3 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS ir3_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS ii DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS ii_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS ii1 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS ii1_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS ii2 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS ii2_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS ii3 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS ii3_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS g DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS g_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS g1 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS g1_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS g2 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS g2_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS g3 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS g3_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS b DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS b_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS b1 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS b1_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS b2 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS b2_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS b3 DOUBLE PRECISION;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS b3_prof JSONB;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS n_customers INTEGER;")
        cur.execute("ALTER TABLE loads ADD COLUMN IF NOT EXISTS n_customers_prof JSONB;")
    conn.commit()


def upsert(conn, grid_id: int, loads: list[dict]) -> int:
    def adapt(val):
        if isinstance(val, dict):
            return Json(val)
        return val
    
    rows = [
        (
            grid_id,
            l.get("idtag"),
            l.get("name"),
            l.get("code"),
            l.get("bus"),
            bool(l.get("active", True)),
            l.get("P"),
            l.get("Q"),
            l.get("conn"),
            l.get("longitude"),
            l.get("latitude"),
            l.get("rdfid"),
            l.get("action"),
            l.get("comment"),
            l.get("modelling_authority"),
            l.get("commissioned_date"),
            l.get("decommissioned_date"),
            adapt(l.get("active_prof")),
            l.get("mttf"),
            l.get("mttr"),
            l.get("capex"),
            l.get("opex"),
            l.get("build_status"),
            l.get("Cost"),
            adapt(l.get("Cost_prof")),
            l.get("facility"),
            adapt(l.get("technologies")),
            l.get("scalable"),
            l.get("shift_key"),
            adapt(l.get("shift_key_prof")),
            l.get("use_kw"),
            adapt(l.get("rms_model")),
            l.get("bus_pos"),
            adapt(l.get("P_prof")),
            l.get("Pa"),
            adapt(l.get("Pa_prof")),
            l.get("Pb"),
            adapt(l.get("Pb_prof")),
            l.get("Pc"),
            adapt(l.get("Pc_prof")),
            adapt(l.get("Q_prof")),
            l.get("Qa"),
            adapt(l.get("Qa_prof")),
            l.get("Qb"),
            adapt(l.get("Qb_prof")),
            l.get("Qc"),
            adapt(l.get("Qc_prof")),
            l.get("Ir"),
            adapt(l.get("Ir_prof")),
            l.get("Ir1"),
            adapt(l.get("Ir1_prof")),
            l.get("Ir2"),
            adapt(l.get("Ir2_prof")),
            l.get("Ir3"),
            adapt(l.get("Ir3_prof")),
            l.get("Ii"),
            adapt(l.get("Ii_prof")),
            l.get("Ii1"),
            adapt(l.get("Ii1_prof")),
            l.get("Ii2"),
            adapt(l.get("Ii2_prof")),
            l.get("Ii3"),
            adapt(l.get("Ii3_prof")),
            l.get("G"),
            adapt(l.get("G_prof")),
            l.get("G1"),
            adapt(l.get("G1_prof")),
            l.get("G2"),
            adapt(l.get("G2_prof")),
            l.get("G3"),
            adapt(l.get("G3_prof")),
            l.get("B"),
            adapt(l.get("B_prof")),
            l.get("B1"),
            adapt(l.get("B1_prof")),
            l.get("B2"),
            adapt(l.get("B2_prof")),
            l.get("B3"),
            adapt(l.get("B3_prof")),
            l.get("n_customers"),
            adapt(l.get("n_customers_prof")),
        )
        for l in (loads or [])
        if l.get("idtag") and l.get("bus")
    ]
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO loads (
                grid_id, idtag, name, code, bus_idtag, active, p, q, conn, longitude, latitude,
                rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
                active_prof, mttf, mttr, capex, opex, build_status, cost, cost_prof, facility,
                technologies, scalable, shift_key, shift_key_prof, use_kw, rms_model, bus_pos,
                p_prof, pa, pa_prof, pb, pb_prof, pc, pc_prof, q_prof, qa, qa_prof, qb, qb_prof,
                qc, qc_prof, ir, ir_prof, ir1, ir1_prof, ir2, ir2_prof, ir3, ir3_prof, ii, ii_prof,
                ii1, ii1_prof, ii2, ii2_prof, ii3, ii3_prof, g, g_prof, g1, g1_prof, g2, g2_prof,
                g3, g3_prof, b, b_prof, b1, b1_prof, b2, b2_prof, b3, b3_prof, n_customers, n_customers_prof
            ) VALUES %s
            ON CONFLICT (grid_id, idtag) DO UPDATE SET
                name = EXCLUDED.name,
                code = EXCLUDED.code,
                bus_idtag = EXCLUDED.bus_idtag,
                active = EXCLUDED.active,
                p = EXCLUDED.p,
                q = EXCLUDED.q,
                conn = EXCLUDED.conn,
                longitude = EXCLUDED.longitude,
                latitude = EXCLUDED.latitude,
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
                use_kw = EXCLUDED.use_kw,
                rms_model = EXCLUDED.rms_model,
                bus_pos = EXCLUDED.bus_pos,
                p_prof = EXCLUDED.p_prof,
                pa = EXCLUDED.pa,
                pa_prof = EXCLUDED.pa_prof,
                pb = EXCLUDED.pb,
                pb_prof = EXCLUDED.pb_prof,
                pc = EXCLUDED.pc,
                pc_prof = EXCLUDED.pc_prof,
                q_prof = EXCLUDED.q_prof,
                qa = EXCLUDED.qa,
                qa_prof = EXCLUDED.qa_prof,
                qb = EXCLUDED.qb,
                qb_prof = EXCLUDED.qb_prof,
                qc = EXCLUDED.qc,
                qc_prof = EXCLUDED.qc_prof,
                ir = EXCLUDED.ir,
                ir_prof = EXCLUDED.ir_prof,
                ir1 = EXCLUDED.ir1,
                ir1_prof = EXCLUDED.ir1_prof,
                ir2 = EXCLUDED.ir2,
                ir2_prof = EXCLUDED.ir2_prof,
                ir3 = EXCLUDED.ir3,
                ir3_prof = EXCLUDED.ir3_prof,
                ii = EXCLUDED.ii,
                ii_prof = EXCLUDED.ii_prof,
                ii1 = EXCLUDED.ii1,
                ii1_prof = EXCLUDED.ii1_prof,
                ii2 = EXCLUDED.ii2,
                ii2_prof = EXCLUDED.ii2_prof,
                ii3 = EXCLUDED.ii3,
                ii3_prof = EXCLUDED.ii3_prof,
                g = EXCLUDED.g,
                g_prof = EXCLUDED.g_prof,
                g1 = EXCLUDED.g1,
                g1_prof = EXCLUDED.g1_prof,
                g2 = EXCLUDED.g2,
                g2_prof = EXCLUDED.g2_prof,
                g3 = EXCLUDED.g3,
                g3_prof = EXCLUDED.g3_prof,
                b = EXCLUDED.b,
                b_prof = EXCLUDED.b_prof,
                b1 = EXCLUDED.b1,
                b1_prof = EXCLUDED.b1_prof,
                b2 = EXCLUDED.b2,
                b2_prof = EXCLUDED.b2_prof,
                b3 = EXCLUDED.b3,
                b3_prof = EXCLUDED.b3_prof,
                n_customers = EXCLUDED.n_customers,
                n_customers_prof = EXCLUDED.n_customers_prof
            """,
            rows,
        )
    return len(rows)


def get_load_by_id(load_id: int):
    """Return load by its internal ID."""
    conn = get_conn()
    ensure_schema(conn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, grid_id, idtag, name, code, bus_idtag, active, p, q, conn, longitude, latitude,
                       rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
                       active_prof, mttf, mttr, capex, opex, build_status, cost, cost_prof, facility,
                       technologies, scalable, shift_key, shift_key_prof, use_kw, rms_model, bus_pos,
                       p_prof, pa, pa_prof, pb, pb_prof, pc, pc_prof, q_prof, qa, qa_prof, qb, qb_prof,
                       qc, qc_prof, ir, ir_prof, ir1, ir1_prof, ir2, ir2_prof, ir3, ir3_prof, ii, ii_prof,
                       ii1, ii1_prof, ii2, ii2_prof, ii3, ii3_prof, g, g_prof, g1, g1_prof, g2, g2_prof,
                       g3, g3_prof, b, b_prof, b1, b1_prof, b2, b2_prof, b3, b3_prof, n_customers, n_customers_prof
                FROM loads WHERE id = %s;
                """,
                (load_id,),
            )
            return cur.fetchone()
    finally:
        conn.close()


def list_loads():
    """Return all loads ordered by id."""
    conn = get_conn()
    ensure_schema(conn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, grid_id, idtag, name, code, bus_idtag, active, p, q, conn, longitude, latitude,
                       rdfid, action, comment, modelling_authority, commissioned_date, decommissioned_date,
                       active_prof, mttf, mttr, capex, opex, build_status, cost, cost_prof, facility,
                       technologies, scalable, shift_key, shift_key_prof, use_kw, rms_model, bus_pos,
                       p_prof, pa, pa_prof, pb, pb_prof, pc, pc_prof, q_prof, qa, qa_prof, qb, qb_prof,
                       qc, qc_prof, ir, ir_prof, ir1, ir1_prof, ir2, ir2_prof, ir3, ir3_prof, ii, ii_prof,
                       ii1, ii1_prof, ii2, ii2_prof, ii3, ii3_prof, g, g_prof, g1, g1_prof, g2, g2_prof,
                       g3, g3_prof, b, b_prof, b1, b1_prof, b2, b2_prof, b3, b3_prof, n_customers, n_customers_prof
                FROM loads
                ORDER BY id;
                """
            )
            return cur.fetchall()
    finally:
        conn.close()
