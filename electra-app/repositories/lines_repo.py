from psycopg2.extras import execute_values
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
    conn.commit()


def upsert(conn, grid_id: int, lines: list[dict]) -> int:
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
                r, x, b, length
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
                length = EXCLUDED.length
            """,
            rows,
        )
    return len(rows)


def get_line_by_id(line_id: int):
    """Return line by its internal ID."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, grid_id, idtag, name, code, bus_from_idtag, bus_to_idtag, active,
                       r, x, b, length
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
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, grid_id, idtag, name, code, bus_from_idtag, bus_to_idtag, active,
                       r, x, b, length
                FROM lines
                ORDER BY id;
                """
            )
            return cur.fetchall()
    finally:
        conn.close()
