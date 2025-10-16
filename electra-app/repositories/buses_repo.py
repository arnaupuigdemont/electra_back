from psycopg2.extras import execute_values
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
                x DOUBLE PRECISION,
                y DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                latitude DOUBLE PRECISION,
                is_slack BOOLEAN,
                UNIQUE (grid_id, idtag)
            );
            """
        )
        cur.execute("ALTER TABLE buses ADD COLUMN IF NOT EXISTS grid_id INTEGER;")
        cur.execute("ALTER TABLE buses ADD CONSTRAINT IF NOT EXISTS fk_buses_grid FOREIGN KEY (grid_id) REFERENCES grids(id) ON DELETE CASCADE;")
    conn.commit()


def upsert(conn, grid_id: int, buses: list[dict]) -> int:
    rows = [
        (
            grid_id,
            b.get("idtag"),
            b.get("name"),
            b.get("code"),
            b.get("Vnom"),
            b.get("Vm0"),
            b.get("Va0"),
            b.get("x"),
            b.get("y"),
            b.get("longitude"),
            b.get("latitude"),
            bool(b.get("is_slack", False)),
        )
        for b in (buses or [])
        if b.get("idtag")
    ]
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO buses (
                grid_id, idtag, name, code, vnom, vm0, va0, x, y, longitude, latitude, is_slack
            ) VALUES %s
            ON CONFLICT (grid_id, idtag) DO UPDATE SET
                name = EXCLUDED.name,
                code = EXCLUDED.code,
                vnom = EXCLUDED.vnom,
                vm0 = EXCLUDED.vm0,
                va0 = EXCLUDED.va0,
                x = EXCLUDED.x,
                y = EXCLUDED.y,
                longitude = EXCLUDED.longitude,
                latitude = EXCLUDED.latitude,
                is_slack = EXCLUDED.is_slack
            """,
            rows,
        )
    return len(rows)


def get_bus_by_id(bus_id: int):
    """Return a single bus row by its primary key id, or None if not found."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, grid_id, idtag, name, code, vnom, vm0, va0, x, y, longitude, latitude, is_slack
                FROM buses
                WHERE id = %s;
                """,
                (bus_id,),
            )
            row = cur.fetchone()
            return row
    finally:
        conn.close()
