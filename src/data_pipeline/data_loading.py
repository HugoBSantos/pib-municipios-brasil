import psycopg
from duckdb import DuckDBPyConnection
from dotenv import load_dotenv
import os

load_dotenv()
POSTGRES_URL = os.getenv("POSTGRES_URL")

def load_to_postgres(connection: DuckDBPyConnection, schema: str, tables: list[str]):
    POSTGRES_DDL_PATH = f"sql/ddl/create_{schema}_postgres.sql"
    
    try:
        with psycopg.connect(POSTGRES_URL) as pg_conn:
            with pg_conn.cursor() as cur:
                with open(POSTGRES_DDL_PATH, mode="r") as f:
                    cur.execute(f.read())
            pg_conn.commit()
        
        connection.execute(f"""
            ATTACH '{POSTGRES_URL}' AS postgres
            (TYPE postgres)
        """)
        
        for table in tables:
            duckdb_table = f"{schema}.{table}"
            postgres_table = f"postgres.{duckdb_table}"
            
            connection.execute(f"""
                INSERT INTO {postgres_table}
                SELECT * FROM {duckdb_table}
            """)
        
    except Exception as e:
        raise TypeError("Failed to load data to PostgresSQL") from e