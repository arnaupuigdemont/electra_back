from psycopg2.extras import execute_values
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
        cur.execute("ALTER TABLE generators ADD CONSTRAINT IF NOT EXISTS fk_generators_grid FOREIGN KEY (grid_id) REFERENCES grids(id) ON DELETE CASCADE;")
    conn.commit()


def upsert(conn, grid_id: int, generators: list[dict]) -> int:
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
                grid_id, idtag, name, code, bus_idtag, active, p, vset, qmin, qmax, pf
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
                pf = EXCLUDED.pf
            """,
            rows,
        )
    return len(rows)


def get_generator_by_id(generator_id: int):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, grid_id, idtag, name, code, bus_idtag, active, p, vset, qmin, qmax, pf
                FROM generators WHERE id = %s;
                """,
                (generator_id,),
            )
            return cur.fetchone()
    finally:
        conn.close()
