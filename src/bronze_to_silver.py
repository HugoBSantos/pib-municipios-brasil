import duckdb
from openpyxl import load_workbook

BRONZE_PATH = "data/bronze/tabela5938.xlsx"
SILVER_DDL_PATH = "sql/ddl/create_silver.sql"

def create_silver():

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL excel; LOAD excel;")
    
    ##### Bronze (Temp) #####
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    
    wb = load_workbook(filename=BRONZE_PATH, read_only=True, keep_links=False)
    sheets = {
        "pib": wb.sheetnames[0],
        "imp": wb.sheetnames[1],
        "valor_add": wb.sheetnames[2]
    }
    
    for k, v in sheets.items():
        bronze_table = f"bronze.{k}"
        bronze_sheet = v
        
        conn.execute(f"""
            CREATE OR REPLACE TABLE {bronze_table} AS
            SELECT *
            FROM read_xlsx(
                '{BRONZE_PATH}',
                sheet='{bronze_sheet}',
                range='A4:W5601',
                header=true,
                all_varchar=true
            );
        """)
    
    ##### Silver #####
    with open(SILVER_DDL_PATH, mode="r") as f:
        conn.execute(f.read())
        
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS silver.dim_municipio AS (
            WITH municipios AS (
                SELECT
                    ROW_NUMBER() OVER() AS municipio_id,
                    SUBSTRING(C0 FROM 1 FOR LENGTH(C0) - 5) AS nome_municipio,
                    RIGHT(C0, 3)[1:2] AS sigla_uf
                FROM bronze.pib
                WHERE C0 LIKE '%)'
            )
            
            SELECT
                m.municipio_id,
                m.nome_municipio,
                CASE
                    WHEN m.sigla_uf IS NOT NULL
                    THEN (
                        SELECT u.uf_id
                        FROM silver.dim_uf u
                        WHERE u.sigla_uf = m.sigla_uf
                    )
                END AS uf_id
            FROM municipios m
        )
    """)

    conn.sql(f"SELECT DISTINCT uf_id FROM silver.dim_municipio ORDER BY uf_id").show()