from psycopg2.extras import execute_values
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
    conn.commit()


def upsert(conn, grid_id: int, transformers: list[dict]) -> int:
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
                r, x, g, b, hv, lv, sn
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
                sn = EXCLUDED.sn
            """,
            rows,
        )
    return len(rows)


def get_transformer2w_by_id(transformer_id: int):
    """Return 2-winding transformer by its internal ID."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, grid_id, idtag, name, code, bus_from_idtag, bus_to_idtag, active,
                       r, x, g, b, hv, lv, sn
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
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, grid_id, idtag, name, code, bus_from_idtag, bus_to_idtag, active,
                       r, x, g, b, hv, lv, sn
                FROM transformers2w
                ORDER BY id;
                """
            )
            return cur.fetchall()
    finally:
        conn.close()
