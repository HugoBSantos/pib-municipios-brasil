INSERT INTO
    silver.anos (ano_id, ano)
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
) AS t(ano);

INSERT INTO
    silver.ufs (uf_id, nome_uf, sigla_uf)
SELECT
    ROW_NUMBER() OVER() AS uf_id,
    *
FROM (
    VALUES
        ('Acre', 'AC'),
        ('Alagoas', 'AL'),
        ('Amazonas', 'AM'),
        ('Amapá', 'AP'),
        ('Bahia', 'BA'),
        ('Ceará', 'CE'),
        ('Distrito Federal', 'DF'),
        ('Espírito Santo', 'ES'),
        ('Goiás', 'GO'),
        ('Maranhão', 'MA'),
        ('Minas Gerais', 'MG'),
        ('Mato Grosso do Sul', 'MS'),
        ('Mato Grosso', 'MT'),
        ('Pará', 'PA'),
        ('Paraíba', 'PB'),
        ('Pernambuco', 'PE'),
        ('Piauí', 'PI'),
        ('Paraná', 'PR'),
        ('Rio de Janeiro', 'RJ'),
        ('Rio Grande do Norte', 'RN'),
        ('Rondônia', 'RO'),
        ('Roraima', 'RR'),
        ('Rio Grande do Sul', 'RS'),
        ('Santa Catarina', 'SC'),
        ('Sergipe', 'SE'),
        ('São Paulo', 'SP'),
        ('Tocantins', 'TO')
) AS t(nome_uf, sigla_uf);