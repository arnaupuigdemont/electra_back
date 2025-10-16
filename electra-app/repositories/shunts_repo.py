from psycopg2.extras import execute_values
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
    conn.commit()


def upsert(conn, grid_id: int, shunts: list[dict]) -> int:
    rows = [
        (
            grid_id,
            s.get("idtag"),
            s.get("name"),
            s.get("code"),
            s.get("bus"),
            bool(s.get("active", True)),
            s.get("B"),
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
                grid_id, idtag, name, code, bus_idtag, active, b
            ) VALUES %s
            ON CONFLICT (grid_id, idtag) DO UPDATE SET
                name = EXCLUDED.name,
                code = EXCLUDED.code,
                bus_idtag = EXCLUDED.bus_idtag,
                active = EXCLUDED.active,
                b = EXCLUDED.b
            """,
            rows,
        )
    return len(rows)


def get_shunt_by_id(shunt_id: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, grid_id, idtag, name, code, bus_idtag, active, b
                FROM shunts WHERE id = %s;
                """,
                (shunt_id,),
            )
            return cur.fetchone()
    finally:
        conn.close()
