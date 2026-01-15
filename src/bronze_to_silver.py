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
            SELECT 
                "Unidade da Federação e Município" AS localidade,
                Ano AS ano_2002,
                C2 AS ano_2003,
                "_1" AS ano_2004, "_2" AS ano_2005, "_3" AS ano_2006, "_4" AS ano_2007, "_5" AS ano_2008,
                "_6" AS ano_2009, "_7" AS ano_2010, "_8" AS ano_2011, "_9" AS ano_2012, "_10" AS ano_2013,
                "_11" AS ano_2014, "_12" AS ano_2015, "_13" AS ano_2016, "_14" AS ano_2017, "_15" AS ano_2018,
                "_16" AS ano_2019, "_17" AS ano_2020, "_18" AS ano_2021, "_19" AS ano_2022, "_20" AS ano_2023
            FROM read_xlsx(
                '{BRONZE_PATH}',
                sheet='{bronze_sheet}',
                range='A3:W5601',
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
                    SUBSTRING(localidade FROM 1 FOR LENGTH(localidade) - 5) AS nome_municipio,
                    RIGHT(localidade, 3)[1:2] AS sigla_uf
                FROM bronze.pib
                WHERE localidade LIKE '%)'
            )
            
            SELECT
                m.municipio_id,
                m.nome_municipio,
                (SELECT u.uf_id
                FROM silver.dim_uf u
                WHERE u.sigla_uf = m.sigla_uf)
                AS uf_id
            FROM municipios m
        )
    """)