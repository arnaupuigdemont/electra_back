from db.db import get_conn


def db_health_check() -> dict:
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                _ = cur.fetchone()
            return {"status": "ok"}
        finally:
            conn.close()
    except Exception as e:
        return {"status": "error", "error": f"{type(e).__name__}: {str(e)}"}
