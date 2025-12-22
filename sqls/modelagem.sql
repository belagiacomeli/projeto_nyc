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
  format = 'CSV',
  uris = ['gs://projeto_nyc/landing/nyc_311/*.csv'],
  skip_leading_rows = 1,
  field_delimiter = ',',
  allow_quoted_newlines = true,
  allow_jagged_rows = true
);

-- Validando a integridade da ingestão
SELECT
  MIN(SAFE_CAST(created_date AS TIMESTAMP)) AS min_created_date,
  MAX(SAFE_CAST(created_date AS TIMESTAMP)) AS max_created_date,
  COUNT(*) AS total_registros
FROM `meicansoft-prd.projeto_nyc_vpn.landing_nyc_311`;


-- Criando e inserindo registros na tabela bruta (raw) a partir da landing
-- Fazendo algumas transformações necessárias nos dados 
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
FROM `meicansoft-prd.projeto_nyc_vpn.landing_nyc_311`;

