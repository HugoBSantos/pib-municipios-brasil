CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.dim_uf AS (
    SELECT
        ROW_NUMBER() OVER() AS uf_id,
        *
    FROM (
        VALUES
            ('AC', 'Acre'),
            ('AL', 'Alagoas'),
            ('AM', 'Amazonas'),
            ('AP', 'Amapá'),
            ('BA', 'Bahia'),
            ('CE', 'Ceará'),
            ('DF', 'Distrito Federal'),
            ('ES', 'Espírito Santo'),
            ('GO', 'Goiás'),
            ('MA', 'Maranhão'),
            ('MG', 'Minas Gerais'),
            ('MS', 'Mato Grosso do Sul'),
            ('MT', 'Mato Grosso'),
            ('PA', 'Pará'),
            ('PB', 'Paraíba'),
            ('PE', 'Pernambuco'),
            ('PI', 'Piauí'),
            ('PR', 'Paraná'),
            ('RJ', 'Rio de Janeiro'),
            ('RN', 'Rio Grande do Norte'),
            ('RO', 'Rondônia'),
            ('RR', 'Roraima'),
            ('RS', 'Rio Grande do Sul'),
            ('SC', 'Santa Catarina'),
            ('SE', 'Sergipe'),
            ('SP', 'São Paulo'),
            ('TO', 'Tocantins')
    ) AS t(sigla_uf, nome_uf)
);

CREATE TABLE IF NOT EXISTS silver.dim_tempo AS (
    SELECT
        ROW_NUMBER() OVER() AS ano_id,
        *
    FROM (
        VALUES
            (2002), (2003), (2004), (2005), (2006),
            (2007), (2008), (2009), (2010), (2011),
            (2012), (2013), (2014), (2015), (2016),
            (2017), (2018), (2019), (2020), (2021),
            (2022), (2023)
    ) AS t(ano)
);