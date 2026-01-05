import os # Usado para apagar arquivos temporários do computador
import requests # Usado para buscar dados da API do NYC 311
from datetime import datetime # Usado para trabalhar com datas
from google.cloud import storage # Usado para acessar o Google Cloud Storage
from google.oauth2 import service_account # Usado para autenticação no Google Cloud
import argparse # Usado para receber parâmetros pela linha de comando

BASE_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv" # URL da API do NYC 311
LIMIT = 50000 # Quantidade máxima de registros por requisição

BUCKET_NAME = "projeto_nyc" # Nome do bucket no Google Cloud Storage

# FUNÇÕES AUXILIARES
def parse_data(data_str: str) -> datetime: # Converte uma data no formato dd/mm/aaaa para datetime
    return datetime.strptime(data_str, "%d/%m/%Y")

def gerar_intervalos_mensais(data_inicio: datetime, data_fim: datetime): # Divide o período total em intervalos mês a mês
    intervalos = []
    atual = datetime(data_inicio.year, data_inicio.month, 1)
    while atual <= data_fim: # Define o primeiro dia do próximo mês
        if atual.month == 12:
            prox = datetime(atual.year + 1, 1, 1)
        else:
            prox = datetime(atual.year, atual.month + 1, 1)
        inicio = max(atual, data_inicio) # Garante que o intervalo respeite o período informado
        fim = min(prox, data_fim)
        intervalos.append((inicio, fim))
        atual = prox
    return intervalos

def arquivo_existe(bucket, blob_name: str) -> bool: # Verifica se o arquivo já existe no bucket
    return bucket.blob(blob_name).exists()

# FUNÇÃO DE INGESTÃO
def baixar_e_enviar_mes(bucket, inicio: datetime, fim: datetime): # Faz a ingestão dos dados de um mês específico
    year = inicio.year
    month = inicio.month
    offset = 0
    total_rows = 0

    print(f"\nProcessando {year}-{month:02d}")
    where = f'created_date >= "{inicio.isoformat()}" AND created_date < "{fim.isoformat()}"'   # Filtro de datas usado na API

    while True:
        gcs_blob_name = f'landing/nyc_311/nyc_311_{year}_{month:02d}_offset_{offset}.csv' # Nome do arquivo no GCS

        if arquivo_existe(bucket, gcs_blob_name): # Pula o download se o arquivo já existir
            print(f'Já existe | offset {offset}')
            offset += LIMIT
            continue

        params = {"$where": where, "$limit": LIMIT, "$offset": offset} # Parâmetros da requisição
        resp = requests.get(BASE_URL, params=params, timeout=120)

        if resp.status_code != 200:  # Verifica erro na requisição
            print(f'Erro HTTP {resp.status_code} | offset {offset}')
            break

        linhas = resp.text.strip().split("\n")  # Separa as linhas do CSV
        if len(linhas) <= 1:  #Se só tiver o cabeçalho, encerra
            break

        num_rows = len(linhas) - 1
        filename = f'nyc_311_{year}_{month:02d}_offset_{offset}.csv'

        with open(filename, "w", encoding="utf-8") as f:  # Salva o arquivo temporariamente
            f.write(resp.text)

        bucket.blob(gcs_blob_name).upload_from_filename(filename) # Envia o arquivo para o Google Cloud Storage
        os.remove(filename) # Remove o arquivo local após o upload

        print(f'Enviado {num_rows} registros | offset {offset}')
        total_rows += num_rows
        offset += LIMIT

    print(f'{year}-{month:02d} finalizado com {total_rows} registros')


# FUNÇÃO PRINCIPAL
def run_ingestion(data_inicio: str, data_fim: str, service_account_path: str): # Função que coordena todo o processo de ingestão
    inicio = parse_data(data_inicio)
    fim = parse_data(data_fim)

    credentials = service_account.Credentials.from_service_account_file(service_account_path) # Faz a autenticação no Google Cloud
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(BUCKET_NAME)

    for inicio_mes, fim_mes in gerar_intervalos_mensais(inicio, fim):   # Executa a ingestão mês a mês
        baixar_e_enviar_mes(bucket, inicio_mes, fim_mes)

# EXECUÇÃO VIA LINHA DE COMANDO
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingestão de dados NYC 311 para GCS")  # Lê os parâmetros passados na execução do script
    parser.add_argument("--data_inicio", required=True, help="Data inicial do período no formato dd/mm/aaaa")
    parser.add_argument("--data_fim", required=True, help="Data final do período no formato dd/mm/aaaa")
    parser.add_argument("--service_account_path", required=True, help="Caminho para o arquivo JSON da service account do GCS")

    args = parser.parse_args()
# Inicia a ingestão
    run_ingestion(
        data_inicio=args.data_inicio,
        data_fim=args.data_fim,
        service_account_path=args.service_account_path
    )
