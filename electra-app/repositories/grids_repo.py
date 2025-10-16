from typing import List, Optional
from db.db import get_conn


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS grids (
                id SERIAL PRIMARY KEY,
                name TEXT,
                base_mva DOUBLE PRECISION,
                raw_json JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        # Ensure new columns exist on existing deployments
        cur.execute("ALTER TABLE grids ADD COLUMN IF NOT EXISTS tmp_file_path TEXT;")
    conn.commit()


def insert_grid(name: Optional[str], base_mva: Optional[float], raw_json: str, tmp_file_path: Optional[str] = None) -> int:
    conn = get_conn()
    try:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO grids (name, base_mva, raw_json, tmp_file_path) VALUES (%s, %s, %s, %s) RETURNING id;",
                (name, base_mva, raw_json, tmp_file_path),
            )
            grid_id = cur.fetchone()[0]
        conn.commit()
        return grid_id
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def list_grid_ids() -> List[int]:
    conn = get_conn()
    try:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM grids ORDER BY created_at DESC, id DESC;")
            rows = cur.fetchall()
        # rows are tuples (id,) unless RealDictCursor used
        return [row[0] if not isinstance(row, dict) else row["id"] for row in rows]
    finally:
        conn.close()


def get_tmp_file_path(grid_id: int) -> Optional[str]:
    conn = get_conn()
    try:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT tmp_file_path FROM grids WHERE id = %s;", (grid_id,))
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def delete_grid(grid_id: int) -> None:
    conn = get_conn()
    try:
        ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute("DELETE FROM grids WHERE id = %s;", (grid_id,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
