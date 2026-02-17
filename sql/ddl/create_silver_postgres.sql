CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.ufs (
    uf_id SERIAL NOT NULL,
    nome_uf VARCHAR(20) NOT NULL,
    sigla_uf CHAR(2) NOT NULL,

    CONSTRAINT pk_uf_id PRIMARY KEY (uf_id),
    CONSTRAINT un_uf_nome UNIQUE (nome_uf),
    CONSTRAINT un_uf_sigla UNIQUE (sigla_uf)
);

CREATE TABLE IF NOT EXISTS silver.municipios (
    municipio_id SERIAL NOT NULL,
    nome_municipio VARCHAR(50) NOT NULL,
    uf_id INTEGER NOT NULL,

    CONSTRAINT pk_mun_id PRIMARY KEY (municipio_id),
    CONSTRAINT fk_mun_uf FOREIGN KEY (uf_id) REFERENCES silver.ufs (uf_id)
);

CREATE TABLE IF NOT EXISTS silver.anos (
    ano_id SERIAL NOT NULL,
    ano INTEGER NOT NULL,

    CONSTRAINT pk_ano_id PRIMARY KEY (ano_id),
    CONSTRAINT un_ano UNIQUE (ano)
);

CREATE TABLE IF NOT EXISTS silver.pib (
    municipio_id INTEGER NOT NULL,
    ano_id INTEGER NOT NULL,
    valor_pib NUMERIC(15,3),

    CONSTRAINT pk_pib_id PRIMARY KEY (municipio_id, ano_id),
    CONSTRAINT fk_pib_mun FOREIGN KEY (municipio_id) REFERENCES silver.municipios (municipio_id),
    CONSTRAINT fk_pib_ano FOREIGN KEY (ano_id) REFERENCES silver.anos (ano_id)
);

CREATE TABLE IF NOT EXISTS silver.impostos (
    municipio_id INTEGER NOT NULL,
    ano_id INTEGER NOT NULL,
    valor_imp NUMERIC(15,3),

    CONSTRAINT pk_imp_id PRIMARY KEY (municipio_id, ano_id),
    CONSTRAINT fk_imp_mun FOREIGN KEY (municipio_id) REFERENCES silver.municipios (municipio_id),
    CONSTRAINT fk_imp_ano FOREIGN KEY (ano_id) REFERENCES silver.anos (ano_id)
);

CREATE TABLE IF NOT EXISTS silver.valor_adicionado (
    municipio_id INTEGER NOT NULL,
    ano_id INTEGER NOT NULL,
    valor_add NUMERIC(15,3),

    CONSTRAINT pk_add_id PRIMARY KEY (municipio_id, ano_id),
    CONSTRAINT fk_add_mun FOREIGN KEY (municipio_id) REFERENCES silver.municipios (municipio_id),
    CONSTRAINT fk_add_ano FOREIGN KEY (ano_id) REFERENCES silver.anos (ano_id)
);

TRUNCATE TABLE
    silver.anos,
    silver.ufs
CASCADE;