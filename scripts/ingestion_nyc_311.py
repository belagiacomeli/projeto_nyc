import os
import requests
from datetime import datetime
from google.cloud import storage
from google.oauth2 import service_account
import argparse

BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"
LIMIT = 50000

# Nome do bucket fixo
BUCKET_NAME = "projeto_nyc"

def parse_data(data_str: str) -> datetime:
    return datetime.strptime(data_str, "%d/%m/%Y")

def gerar_intervalos_mensais(data_inicio: datetime, data_fim: datetime):
    intervalos = []
    atual = datetime(data_inicio.year, data_inicio.month, 1)
    while atual <= data_fim:
        if atual.month == 12:
            prox = datetime(atual.year + 1, 1, 1)
        else:
            prox = datetime(atual.year, atual.month + 1, 1)
        inicio = max(atual, data_inicio)
        fim = min(prox, data_fim)
        intervalos.append((inicio, fim))
        atual = prox
    return intervalos

def arquivo_existe(bucket, blob_name: str) -> bool:
    return bucket.blob(blob_name).exists()

def baixar_e_enviar_mes(bucket, inicio: datetime, fim: datetime):
    year = inicio.year
    month = inicio.month
    offset = 0
    total_rows = 0

    print(f"\nProcessando {year}-{month:02d}")
    where = f'created_date >= "{inicio.isoformat()}" AND created_date < "{fim.isoformat()}"'

    while True:
        gcs_blob_name = f'landing/nyc_311/nyc_311_{year}_{month:02d}_offset_{offset}.csv'

        if arquivo_existe(bucket, gcs_blob_name):
            print(f'Já existe | offset {offset}')
            offset += LIMIT
            continue

        params = {"$where": where, "$limit": LIMIT, "$offset": offset}
        resp = requests.get(BASE_URL, params=params, timeout=120)

        if resp.status_code != 200:
            print(f'Erro HTTP {resp.status_code} | offset {offset}')
            break

        linhas = resp.text.strip().split("\n")
        if len(linhas) <= 1:
            break

        num_rows = len(linhas) - 1
        filename = f'nyc_311_{year}_{month:02d}_offset_{offset}.csv'

        with open(filename, "w", encoding="utf-8") as f:
            f.write(resp.text)

        bucket.blob(gcs_blob_name).upload_from_filename(filename)
        os.remove(filename)

        print(f'Enviado {num_rows} registros | offset {offset}')
        total_rows += num_rows
        offset += LIMIT

    print(f'{year}-{month:02d} finalizado com {total_rows} registros')

def run_ingestion(data_inicio: str, data_fim: str, service_account_path: str):
    inicio = parse_data(data_inicio)
    fim = parse_data(data_fim)

    credentials = service_account.Credentials.from_service_account_file(service_account_path)
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(BUCKET_NAME)

    for inicio_mes, fim_mes in gerar_intervalos_mensais(inicio, fim):
        baixar_e_enviar_mes(bucket, inicio_mes, fim_mes)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingestão de dados NYC 311 para GCS")
    parser.add_argument("--data_inicio", required=True, help="Data inicial do período no formato dd/mm/aaaa")
    parser.add_argument("--data_fim", required=True, help="Data final do período no formato dd/mm/aaaa")
    parser.add_argument("--service_account_path", required=True, help="Caminho para o arquivo JSON da service account do GCS")

    args = parser.parse_args()

    run_ingestion(
        data_inicio=args.data_inicio,
        data_fim=args.data_fim,
        service_account_path=args.service_account_path
    )
