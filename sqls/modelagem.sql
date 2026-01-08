-- Criando tabela landing como externa 
-- O objetivo é ler os arquivos do bucket GCS para tratamento dentro do BigQuery
-- Criando tabela bruta (raw) a partir da landing
CREATE OR REPLACE EXTERNAL TABLE `meicansoft-prd.projeto_nyc_vpn.landing_nyc_311` (
  unique_key STRING,
  created_date STRING,
  closed_date STRING,
  agency STRING,
  agency_name STRING,
  complaint_type STRING,
  descriptor STRING,
  location_type STRING,
  incident_zip STRING, 
  incident_address STRING,
  street_name STRING,
  cross_street_1 STRING,
  cross_street_2 STRING,
  intersection_street_1 STRING,
  intersection_street_2 STRING,
  address_type STRING,
  city STRING,
  landmark STRING,
  facility_type STRING,
  status STRING,
  due_date STRING,
  resolution_description STRING,
  resolution_action_updated_date STRING,
  community_board STRING,
  bbl STRING,
  borough STRING,
  x_coordinate_state_plane STRING,
  y_coordinate_state_plane STRING,
  open_data_channel_type STRING,
  park_facility_name STRING,
  park_borough STRING,
  vehicle_type STRING,
  taxi_company_borough STRING,
  taxi_pick_up_location STRING,
  bridge_highway_name STRING,
  bridge_highway_direction STRING,
  road_ramp STRING,
  bridge_highway_segment STRING,
  latitude STRING,
  longitude STRING,
  location STRING
);
OPTIONS (
  format = 'CSV',                                     -- Define que o formato dos arquivos de origem é CSV
  uris = ['gs://projeto_nyc/landing/nyc_311/*.csv'],  -- Caminho dos arquivos no Google Cloud Storage
  skip_leading_rows = 1,                              -- Ignora a primeira linha do arquivo CSV (cabeçalho)
  field_delimiter = ',',                              -- Define o delimitador de campo como vírgula
  allow_quoted_newlines = true,                       -- Permite quebras de linha dentro de campos entre aspas
  allow_jagged_rows = true                            -- Permite linhas com número variável de colunas
);

-- Validando a integridade da ingestão
SELECT
  MIN(SAFE_CAST(created_date AS TIMESTAMP)) AS min_created_date,   -- Retorna a menor data/hora de criação dos registros
  MAX(SAFE_CAST(created_date AS TIMESTAMP)) AS max_created_date,   -- Retorna a maior data/hora de criação dos registros
  COUNT(*) AS total_registros                                      -- Conta o total de registros existentes na tabela
FROM `meicansoft-prd.projeto_nyc_vpn.landing_nyc_311`;             -- Tabela de origem na camada Landing


-- Criando e inserindo registros na tabela bruta (raw) a partir da landing
-- Fazendo algumas transformações necessárias nos dados, aplicando conversões de tipos
CREATE OR REPLACE TABLE `meicansoft-prd.projeto_nyc_vpn.raw_tratada_nyc_311` AS
SELECT
  SAFE_CAST(unique_key AS INT64)                             AS chave_unica,
  SAFE_CAST(created_date AS TIMESTAMP)                       AS data_criacao,
  SAFE_CAST(closed_date AS TIMESTAMP)                        AS data_fechamento,
  agency                                                     AS agencia,
  agency_name                                                AS nome_agencia,
  complaint_type                                             AS tipo_reclamacao,
  descriptor                                                 AS descricao,
  location_type                                              AS tipo_local,
  SAFE_CAST(incident_zip AS STRING)                          AS cep_incidente,  
  incident_address                                           AS endereco_incidente,
  street_name                                                AS nome_rua,
  cross_street_1                                             AS rua_cruzamento_1,
  cross_street_2                                             AS rua_cruzamento_2,
  intersection_street_1                                      AS intersecao_rua_1,
  intersection_street_2                                      AS intersecao_rua_2,
  address_type                                               AS tipo_endereco,
  city                                                       AS cidade,
  landmark                                                   AS ponto_referencia,
  facility_type                                              AS tipo_instalacao,
  status                                                     AS status,
  SAFE_CAST(due_date AS TIMESTAMP)                           AS data_vencimento,
  resolution_description                                     AS descricao_resolucao,
  SAFE_CAST(resolution_action_updated_date AS TIMESTAMP)     AS data_atualizacao_resolucao,
  community_board                                            AS conselho_comunitario,
  SAFE_CAST(bbl AS INT64)                                    AS bbl,
  borough                                                    AS bairro,
  SAFE_CAST(x_coordinate_state_plane AS INT64)               AS coordenada_x_estado,
  SAFE_CAST(y_coordinate_state_plane AS INT64)               AS coordenada_y_estado,
  open_data_channel_type                                     AS tipo_canal_abertura,
  park_facility_name                                         AS nome_instalacao_parque,
  park_borough                                               AS bairro_parque,
  vehicle_type                                               AS tipo_veiculo,
  taxi_company_borough                                       AS bairro_empresa_taxi,
  taxi_pick_up_location                                      AS local_retirada_taxi,
  bridge_highway_name                                        AS nome_ponte_rodovia,
  bridge_highway_direction                                   AS direcao_ponte_rodovia,
  road_ramp                                                  AS rampa_rodovia,
  bridge_highway_segment                                     AS segmento_ponte_rodovia,
  SAFE_CAST(latitude AS FLOAT64)                             AS latitude,
  SAFE_CAST(longitude AS FLOAT64)                            AS longitude,
  location                                                   AS localizacao
FROM `meicansoft-prd.projeto_nyc_vpn.landing_nyc_311`;       -- Tabela de origem na camada Landing

-- Criando a tabela de staging tratando apenas as colunas de texto
-- Substituindo nulos por 'Não informado' apenas em colunas STRING 
CREATE OR REPLACE TABLE `meicansoft-prd.projeto_nyc_vpn.staging_nyc_311` AS
SELECT
    -- Campos numéricos e de datas
    chave_unica,
    data_criacao,
    data_fechamento,
    data_vencimento,
    data_atualizacao_resolucao,
    bbl,
    coordenada_x_estado,
    coordenada_y_estado,
    latitude,
    longitude,

    -- Agência
    UPPER(
        CASE
            WHEN agencia IS NULL THEN 'Não informado'
            WHEN agencia IN ('N/A', 'Unspecified', 'UNKNOWN') THEN 'Não informado'
            ELSE agencia
        END
    ) AS agencia,

    -- Tipo de reclamação
    CASE
        WHEN tipo_reclamacao IS NULL THEN NULL
        WHEN REGEXP_CONTAINS(
            tipo_reclamacao,
            r'(?i)eval\(|compile\(|<|>|&quot;|-->|\.php|/'
        ) THEN NULL
        WHEN LENGTH(TRIM(tipo_reclamacao)) < 3 THEN NULL
        ELSE INITCAP(LOWER(tipo_reclamacao))
    END AS tipo_reclamacao,

    -- CEP
    cep_incidente,
    CASE
        WHEN REGEXP_CONTAINS(TRIM(cep_incidente), r'^[0-9]{1,5}$')
            THEN TRIM(cep_incidente)
        ELSE NULL
    END AS cep_incidente_padrao,

    -- Bairro
    CASE
        WHEN bairro IS NULL
         OR bairro IN ('N/A', 'Unspecified', 'UNKNOWN', '')
        THEN 'Não informado'
        ELSE INITCAP(LOWER(bairro))
    END AS bairro,


    -- Canal de abertura
    CASE
        WHEN tipo_canal_abertura IN ('UNKNOWN', 'Unspecified', 'OTHER', 'N/A', '')
            THEN 'Não informado'
        ELSE INITCAP(LOWER(tipo_canal_abertura))
    END AS tipo_canal_abertura,

    -- Status simplificado para análise
    CASE
        WHEN status IN ('Closed', 'Closed - Testing')
            THEN 'Resolvido'
        WHEN status IN ('Cancel', 'Cancelled')
            THEN 'Cancelado'
        WHEN status IN (
            'Open',
            'In Progress',
            'Started',
            'Assigned',
            'Pending',
            'Email Sent',
            'Unassigned',
            'Draft'
        )
            THEN 'Em andamento'
        WHEN status = 'Unspecified'
            THEN 'Não especificado'
        ELSE 'Em andamento'
    END AS status_analise,

    -- Categoria de tempo de resolução
    CASE
        WHEN status IN ('Closed', 'Closed - Testing')
             AND DATE_DIFF(DATE(data_fechamento), DATE(data_criacao), DAY) <= 7
            THEN 'Rápida'

        WHEN status IN ('Closed', 'Closed - Testing')
             AND DATE_DIFF(DATE(data_fechamento), DATE(data_criacao), DAY) <= 30
            THEN 'Média'

        WHEN status IN ('Closed', 'Closed - Testing')
            THEN 'Lenta'

        WHEN status IN (
            'Open',
            'In Progress',
            'Started',
            'Assigned',
            'Pending',
            'Email Sent',
            'Unassigned',
            'Draft'
        )
            THEN 'Em andamento'

        WHEN status IN ('Cancel', 'Cancelled')
            THEN 'Cancelado'

        ELSE 'Não especificado'
    END AS categoria_tempo_resolucao,

    -- Campos textuais padronizados
    INITCAP(LOWER(IFNULL(status, 'Não informado'))) AS status,
    INITCAP(LOWER(IFNULL(nome_agencia, 'Não informado'))) AS nome_agencia,
    INITCAP(LOWER(IFNULL(descricao, 'Não informado'))) AS descricao,
    INITCAP(LOWER(IFNULL(tipo_local, 'Não informado'))) AS tipo_local,
    INITCAP(LOWER(IFNULL(endereco_incidente, 'Não informado'))) AS endereco_incidente,
    INITCAP(LOWER(IFNULL(nome_rua, 'Não informado'))) AS nome_rua,
    INITCAP(LOWER(IFNULL(rua_cruzamento_1, 'Não informado'))) AS rua_cruzamento_1,
    INITCAP(LOWER(IFNULL(rua_cruzamento_2, 'Não informado'))) AS rua_cruzamento_2,
    INITCAP(LOWER(IFNULL(intersecao_rua_1, 'Não informado'))) AS intersecao_rua_1,
    INITCAP(LOWER(IFNULL(intersecao_rua_2, 'Não informado'))) AS intersecao_rua_2,
    INITCAP(LOWER(IFNULL(tipo_endereco, 'Não informado'))) AS tipo_endereco,
    INITCAP(LOWER(IFNULL(cidade, 'Não informado'))) AS cidade,
    INITCAP(LOWER(IFNULL(ponto_referencia, 'Não informado'))) AS ponto_referencia,
    INITCAP(LOWER(IFNULL(tipo_instalacao, 'Não informado'))) AS tipo_instalacao,
    INITCAP(LOWER(IFNULL(descricao_resolucao, 'Não informado'))) AS descricao_resolucao,
    INITCAP(LOWER(IFNULL(conselho_comunitario, 'Não informado'))) AS conselho_comunitario,
    INITCAP(LOWER(IFNULL(nome_instalacao_parque, 'Não informado'))) AS nome_instalacao_parque,
    INITCAP(LOWER(IFNULL(bairro_parque, 'Não informado'))) AS bairro_parque,
    INITCAP(LOWER(IFNULL(tipo_veiculo, 'Não informado'))) AS tipo_veiculo,
    INITCAP(LOWER(IFNULL(bairro_empresa_taxi, 'Não informado'))) AS bairro_empresa_taxi,
    INITCAP(LOWER(IFNULL(local_retirada_taxi, 'Não informado'))) AS local_retirada_taxi,
    INITCAP(LOWER(IFNULL(nome_ponte_rodovia, 'Não informado'))) AS nome_ponte_rodovia,
    INITCAP(LOWER(IFNULL(direcao_ponte_rodovia, 'Não informado'))) AS direcao_ponte_rodovia,
    INITCAP(LOWER(IFNULL(rampa_rodovia, 'Não informado'))) AS rampa_rodovia,
    INITCAP(LOWER(IFNULL(segmento_ponte_rodovia, 'Não informado'))) AS segmento_ponte_rodovia,
    INITCAP(LOWER(IFNULL(localizacao, 'Não informado'))) AS localizacao

FROM `meicansoft-prd.projeto_nyc_vpn.raw_tratada_nyc_311`;

----------------------------------------------------------------------------------------------------------------------------

-- Criação das tabelas trusted a partir da staging

-- Tabela com o total de reclamações por tipo
CREATE OR REPLACE TABLE `meicansoft-prd.projeto_nyc_vpn.trusted__top_tipos_reclamacao` AS
SELECT
  tipo_reclamacao,
  COUNT(*) AS total_reclamacoes
FROM `meicansoft-prd.projeto_nyc_vpn.staging_nyc_311`
WHERE tipo_reclamacao IS NOT NULL
GROUP BY tipo_reclamacao
ORDER BY total_reclamacoes DESC;

-- Tabela com o total de reclamações por bairro
CREATE OR REPLACE TABLE `meicansoft-prd.projeto_nyc_vpn.trusted__reclamacoes_por_bairro` AS
SELECT
  bairro,
  COUNT(*) AS total_reclamacoes
FROM `meicansoft-prd.projeto_nyc_vpn.staging_nyc_311`
GROUP BY bairro
ORDER BY total_reclamacoes DESC;

-- Tabela com quantidade de reclamações por status
CREATE OR REPLACE TABLE `meicansoft-prd.projeto_nyc_vpn.trusted__status_reclamacoes` AS
SELECT
    status_analise AS status_reclamacao,
    COUNT(*) AS total
FROM `meicansoft-prd.projeto_nyc_vpn.staging_nyc_311`
GROUP BY status_analise
ORDER BY total DESC;

-- Tabela com quantidade de dias para resolução das reclamações 
CREATE OR REPLACE TABLE `meicansoft-prd.projeto_nyc_vpn.trusted_tempo_medio_resolucao` AS
WITH base AS (
    SELECT
        chave_unica,
        data_criacao,
        data_fechamento,
        CASE
            WHEN data_fechamento IS NULL THEN 0
            WHEN data_fechamento < data_criacao THEN 0
            WHEN data_fechamento > CURRENT_TIMESTAMP() THEN 0
            WHEN DATE(data_fechamento) < DATE '1901-01-01' THEN 0
            ELSE TIMESTAMP_DIFF(
                data_fechamento,
                data_criacao,
                DAY
            )
        END AS dias_para_resolver
    FROM `meicansoft-prd.projeto_nyc_vpn.staging_nyc_311`
)

SELECT
    chave_unica,
    data_criacao,
    data_fechamento,
    dias_para_resolver,
    CASE
        WHEN dias_para_resolver <= 7 THEN 'Rápida'
        WHEN dias_para_resolver BETWEEN 8 AND 30 THEN 'Média'
        ELSE 'Lenta'
    END AS categoria_tempo_resolucao
FROM base;

-- Tabela com o total de reclamações por agência e nome da agência
CREATE OR REPLACE TABLE `meicansoft-prd.projeto_nyc_vpn.trusted_reclamacoes_por_agencia` AS
SELECT
    agencia,
    nome_agencia,
    COUNT(*) AS total_reclamacoes
FROM `meicansoft-prd.projeto_nyc_vpn.staging_nyc_311`
WHERE agencia IS NOT NULL
GROUP BY
    agencia,
    nome_agencia
ORDER BY total_reclamacoes DESC;

-- Tabela com volume de reclamações por agência e tempo médio de resolução
CREATE OR REPLACE TABLE `meicansoft-prd.projeto_nyc_vpn.trusted_agencia_volume_x_tempo` AS
SELECT
    s.agencia,
    s.nome_agencia,
    COUNT(*) AS total_reclamacoes,
    ROUND(
        AVG(t.dias_para_resolver),
        2
    ) AS tempo_medio_resolucao_dias

FROM `meicansoft-prd.projeto_nyc_vpn.trusted_tempo_medio_resolucao` t
JOIN `meicansoft-prd.projeto_nyc_vpn.staging_nyc_311` s
    ON t.chave_unica = s.chave_unica
WHERE s.agencia IS NOT NULL
GROUP BY
    s.agencia,
    s.nome_agencia
ORDER BY total_reclamacoes DESC;

-- Tabela com o total de casos lentos por agência
CREATE OR REPLACE TABLE `meicansoft-prd.projeto_nyc_vpn.trusted_agencias_casos_lentos` AS
SELECT
    s.agencia,
    s.nome_agencia,
    COUNT(*) AS casos_lentos
FROM `meicansoft-prd.projeto_nyc_vpn.trusted_tempo_medio_resolucao` t
JOIN `meicansoft-prd.projeto_nyc_vpn.staging_nyc_311` s
    ON t.chave_unica = s.chave_unica
WHERE t.categoria_tempo_resolucao = 'Lenta'
GROUP BY
    s.agencia,
    s.nome_agencia
ORDER BY casos_lentos DESC;

-- Tabela geral usada no looker com principais métricas
CREATE OR REPLACE TABLE `meicansoft-prd.projeto_nyc_vpn.trusted_nyc_geral` AS
WITH base_calculada AS (
    SELECT
        chave_unica,
        agencia,
        nome_agencia,
        bairro,
        tipo_reclamacao,
        status_analise AS status_reclamacao,
        data_criacao,
        data_fechamento,
        EXTRACT(YEAR FROM data_criacao) AS ano,
        CASE
            WHEN data_fechamento IS NULL THEN 0
            WHEN data_fechamento < data_criacao THEN 0
            WHEN data_fechamento > CURRENT_TIMESTAMP() THEN 0
            WHEN DATE(data_fechamento) < DATE '1901-01-01' THEN 0
            ELSE TIMESTAMP_DIFF(data_fechamento, data_criacao, DAY)
        END AS dias_para_resolver
    FROM `meicansoft-prd.projeto_nyc_vpn.staging_nyc_311`
)
SELECT
    *,
    CASE
        WHEN dias_para_resolver <= 7 THEN 'Rápida'
        WHEN dias_para_resolver BETWEEN 8 AND 30 THEN 'Média'
        ELSE 'Lenta'
    END AS categoria_tempo_resolucao
FROM base_calculada;

SELECT 
  tipo_reclamacao, 
  COUNT(*) AS quantidade
FROM `meicansoft-prd.projeto_nyc_vpn.trusted_nyc_geral`
WHERE tipo_reclamacao IS NULL OR tipo_reclamacao = ''
GROUP BY 1

SELECT DISTINCT 
  tipo_reclamacao
FROM `meicansoft-prd.projeto_nyc_vpn.trusted_nyc_geral`
ORDER BY tipo_reclamacao ASC

-- Adicionando coluna na staging para ver tipo_canal_abertura

ALTER TABLE `meicansoft-prd.projeto_nyc_vpn.trusted_nyc_geral`
ADD COLUMN tipo_canal_abertura STRING;

UPDATE `meicansoft-prd.projeto_nyc_vpn.trusted_nyc_geral` t
SET tipo_canal_abertura = s.tipo_canal_abertura
FROM (
    SELECT
        chave_unica,
        ANY_VALUE(tipo_canal_abertura) AS tipo_canal_abertura
    FROM `meicansoft-prd.projeto_nyc_vpn.staging_nyc_311`
    GROUP BY chave_unica
) s
WHERE t.chave_unica = s.chave_unica;