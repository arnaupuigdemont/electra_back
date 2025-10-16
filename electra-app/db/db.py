import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "dbname=electra user=electra password=electra host=localhost port=5432")

def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
