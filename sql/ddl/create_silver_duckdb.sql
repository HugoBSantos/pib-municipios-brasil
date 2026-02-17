CREATE SCHEMA silver;

CREATE TABLE silver.ufs (
    uf_id INTEGER,
    nome_uf VARCHAR(20),
    sigla_uf CHAR(2)
);

CREATE TABLE silver.municipios (
    municipio_id INTEGER,
    nome_municipio VARCHAR(50),
    uf_id INTEGER
);

CREATE TABLE silver.anos (
    ano_id INTEGER,
    ano INTEGER
);

CREATE TABLE silver.pib (
    municipio_id INTEGER,
    ano_id INTEGER,
    valor_pib NUMERIC(15,3)
);

CREATE TABLE silver.impostos (
    municipio_id INTEGER,
    ano_id INTEGER,
    valor_imp NUMERIC(15,3)
);

CREATE TABLE silver.valor_adicionado (
    municipio_id INTEGER,
    ano_id INTEGER,
    valor_add NUMERIC(15,3)
);