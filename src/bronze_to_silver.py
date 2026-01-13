import duckdb

BRONZE_PATH = 'data/bronze/tabela5938.xlsx'

def create_silver():

    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL excel; LOAD excel;")

    for sheet in ["pib", "impostos", "valor_adicionado"]:
        conn.execute(f"""
            CREATE OR REPLACE TABLE silver_{sheet} AS
            SELECT *
            FROM read_xlsx(
                '{BRONZE_PATH}',
                sheet='bronze_{sheet}',
                range='A4:W5601',
                header=true,
                all_varchar=true
            )
        """)
    
    conn.sql("SELECT * FROM silver_pib").show()