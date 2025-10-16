import os
import re
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv


_file = Path(__file__).resolve()
_app_root = _file.parents[1]  
_repo_root = _app_root.parent  
_loaded_envs: list[str] = []
for p, override in (( _repo_root / ".env", True), ( _app_root / ".env", False)):
    if p.exists():
        load_dotenv(p, override=override)
        _loaded_envs.append(str(p))


def _normalize_uri(uri: str) -> str:
    
    if not uri:
        return uri
    d = uri.strip()
    if "://" in d:
        d = re.sub(r"^postgresql\+[^:]+://", "postgresql://", d, flags=re.IGNORECASE)
        d = re.sub(r"^postgres://", "postgresql://", d, flags=re.IGNORECASE)
    return d


def _get_conn_from_parts():

    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    dbname = os.getenv("POSTGRES_DB")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", "5432"))

    missing = [name for name, val in (
        ("POSTGRES_USER", user),
        ("POSTGRES_PASSWORD", password),
        ("POSTGRES_DB", dbname),
    ) if not val]
    if missing:
        raise RuntimeError(
            "Missing required database settings: " + ", ".join(missing) +
            ". Set them in the project .env or OS env. Loaded .env files: " + (", ".join(_loaded_envs) or "none")
        )

    return psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port,
        cursor_factory=RealDictCursor,
    )


def get_conn():
    
    return _get_conn_from_parts()
