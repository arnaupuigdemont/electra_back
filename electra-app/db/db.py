import os
import re
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

_this_dir = os.path.dirname(os.path.abspath(__file__))
_app_root = os.path.dirname(_this_dir)  
_repo_root = os.path.dirname(_app_root) 
load_dotenv(os.path.join(_app_root, ".env"))
load_dotenv(os.path.join(_repo_root, ".env"))

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "dbname=electra user=electra password=electra host=localhost port=5432",
)


def _normalize_dsn(dsn: str) -> str:

    if not dsn:
        return dsn

    d = dsn.strip()

    if "://" in d:
        d = re.sub(r"^postgresql\+[^:]+://", "postgresql://", d, flags=re.IGNORECASE)
        d = re.sub(r"^postgres://", "postgresql://", d, flags=re.IGNORECASE)
    return d


def get_conn():
    dsn = _normalize_dsn(DATABASE_URL)
    return psycopg2.connect(dsn, cursor_factory=RealDictCursor)
