from psycopg2.extras import execute_values
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
        cur.execute("ALTER TABLE loads ADD CONSTRAINT IF NOT EXISTS fk_loads_grid FOREIGN KEY (grid_id) REFERENCES grids(id) ON DELETE CASCADE;")
    conn.commit()


def upsert(conn, grid_id: int, loads: list[dict]) -> int:
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
                grid_id, idtag, name, code, bus_idtag, active, p, q, conn, longitude, latitude
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
                latitude = EXCLUDED.latitude
            """,
            rows,
        )
    return len(rows)


def get_load_by_id(load_id: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, grid_id, idtag, name, code, bus_idtag, active, p, q, conn, longitude, latitude
                FROM loads WHERE id = %s;
                """,
                (load_id,),
            )
            return cur.fetchone()
    finally:
        conn.close()
