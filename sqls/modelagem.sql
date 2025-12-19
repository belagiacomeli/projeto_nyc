-- Criando tabela landing como externa 
-- O objetivo é ler os arquivos do bucket GCS para tratamento dentro do BigQuery
CREATE OR REPLACE EXTERNAL TABLE `meicansoft-prd.projeto_nyc_vpn.landing_nyc_311`
OPTIONS (
  format = 'CSV',
  uris = [
    'gs://projeto_nyc/landing/nyc_311/*.csv'
  ],
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

-- Criando tabela bruta (raw) a partir da landing
CREATE OR REPLACE TABLE `meicansoft-prd.projeto_nyc_vpn.nyc_311_raw_tratada` (
  chave_unica INT64,
  data_criacao TIMESTAMP,
  data_fechamento TIMESTAMP,
  agencia STRING,
  nome_agencia STRING,
  tipo_reclamacao STRING,
  descricao STRING,
  tipo_local STRING,
  cep_incidente STRING,
  endereco_incidente STRING,
  nome_rua STRING,
  rua_cruzamento_1 STRING,
  rua_cruzamento_2 STRING,
  intersecao_rua_1 STRING,
  intersecao_rua_2 STRING,
  tipo_endereco STRING,
  cidade STRING,
  ponto_referencia STRING,
  tipo_instalacao STRING,
  status STRING,
  data_vencimento TIMESTAMP,
  descricao_resolucao STRING,
  data_atualizacao_resolucao TIMESTAMP,
  conselho_comunitario STRING,
  bbl INT64,
  bairro STRING,
  coordenada_x_estado INT64,
  coordenada_y_estado INT64,
  tipo_canal_abertura STRING,
  nome_instalacao_parque STRING,
  bairro_parque STRING,
  tipo_veiculo STRING,
  bairro_empresa_taxi STRING,
  local_retirada_taxi STRING,
  nome_ponte_rodovia STRING,
  direcao_ponte_rodovia STRING,
  rampa_rodovia STRING,
  segmento_ponte_rodovia STRING,
  latitude FLOAT64,
  longitude FLOAT64,
  localizacao STRING
);

-- Inserindo registros na tabela bruta (raw) a partir da landing
-- Fazendo algumas transformações necessárias nos dados
INSERT INTO `meicansoft-prd.projeto_nyc_vpn.nyc_311_raw_tratada`
SELECT
  SAFE_CAST(unique_key AS INT64)                           AS chave_unica,
  SAFE_CAST(created_date AS TIMESTAMP)                     AS data_criacao,
  SAFE_CAST(closed_date AS TIMESTAMP)                      AS data_fechamento,
  agency                                                    AS agencia,
  agency_name                                               AS nome_agencia,
  complaint_type                                            AS tipo_reclamacao,
  descriptor                                                AS descricao,
  location_type                                             AS tipo_local,
  incident_zip                                              AS cep_incidente,
  incident_address                                          AS endereco_incidente,
  street_name                                               AS nome_rua,
  cross_street_1                                            AS rua_cruzamento_1,
  cross_street_2                                            AS rua_cruzamento_2,
  intersection_street_1                                     AS intersecao_rua_1,
  intersection_street_2                                     AS intersecao_rua_2,
  address_type                                              AS tipo_endereco,
  city                                                      AS cidade,
  landmark                                                  AS ponto_referencia,
  facility_type                                             AS tipo_instalacao,
  status                                                    AS status,
  SAFE_CAST(due_date AS TIMESTAMP)                          AS data_vencimento,
  resolution_description                                    AS descricao_resolucao,
  SAFE_CAST(resolution_action_updated_date AS TIMESTAMP)   AS data_atualizacao_resolucao,
  community_board                                           AS conselho_comunitario,
  SAFE_CAST(bbl AS INT64)                                   AS bbl,
  borough                                                   AS bairro,
  SAFE_CAST(x_coordinate_state_plane AS INT64)              AS coordenada_x_estado,
  SAFE_CAST(y_coordinate_state_plane AS INT64)              AS coordenada_y_estado,
  open_data_channel_type                                    AS tipo_canal_abertura,
  park_facility_name                                        AS nome_instalacao_parque,
  park_borough                                              AS bairro_parque,
  vehicle_type                                              AS tipo_veiculo,
  taxi_company_borough                                      AS bairro_empresa_taxi,
  taxi_pick_up_location                                     AS local_retirada_taxi,
  bridge_highway_name                                       AS nome_ponte_rodovia,
  bridge_highway_direction                                  AS direcao_ponte_rodovia,
  road_ramp                                                 AS rampa_rodovia,
  bridge_highway_segment                                    AS segmento_ponte_rodovia,
  SAFE_CAST(latitude AS FLOAT64)                             AS latitude,
  SAFE_CAST(longitude AS FLOAT64)                            AS longitude,
  location                                                  AS localizacao
FROM `meicansoft-prd.projeto_nyc_vpn.landing_nyc_311`;

