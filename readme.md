# Documentação do Projeto – Pipeline de Dados NYC 311

**Responsável:** Izabela Pereira Giacomeli  
**Data da última atualização:** 31 de dezembro de 2025  

---

## 🔗 Links Úteis
* **Dashboard:** [Visualizar Looker Studio](https://lookerstudio.google.com/reporting/ab473f47-2316-4d03-8e4e-d00f05a14a02)

---

## 📝 Visão Geral
Este projeto foi desenvolvido com o objetivo de construir um pipeline de dados completo para análise das reclamações do serviço da prefeitura de Nova York, **NYC 311**.

A arquitetura utiliza:
* **Python:** Ingestão de dados via API.
* **Google Cloud Storage (GCS):** Camada de armazenamento (Data Lake).
* **BigQuery:** Processamento, modelagem e Data Warehousing.
* **Looker Studio:** Consumo final e visualização.

![Arquitetura do Pipeline]("Imagens\Diagrama\arquitetura_pipeline.png.png")  
*Figura 1. Arquitetura do pipeline de dados desenvolvido.*

---

## 📂 Fonte de Dados
A fonte de dados deste projeto é o **NYC Open Data – 311 Service Requests**, um portal oficial da cidade de Nova York que disponibiliza informações públicas sobre solicitações e reclamações registradas pelo serviço 311 desde 2010 até o presente.
O conjunto de dados é disponibilizado por meio de uma API RESTful, permitindo acesso programático aos registros em formato estruturado. Cada registro representa uma solicitação de serviço ou reclamação feita pela população e contém informações como data de abertura e fechamento, tipo de reclamação, agência responsável, status, localização e outros atributos relevantes.
Essa fonte foi escolhida por ser oficial, pública, confiável e amplamente utilizada em análises urbanas, além de possuir grande volume de dados, o que a torna ideal para demonstrar um pipeline de dados completo, escalável e orientado à análise.


---

## ⚙️ Ingestão dos Dados

A ingestão dos dados foi realizada via API pública do NYC 311, pois o download direto do arquivo CSV completo pelo site não era viável. O arquivo é muito grande, o carregamento frequentemente não concluía e, quando concluía, o CSV vinha corrompido, inviabilizando o uso.
Para resolver esse problema, foi desenvolvido um código em Python que faz a extração dos dados de forma controlada e incremental. A estratégia adotada foi dividir a ingestão por intervalos mensais e utilizar paginação (`limit` e `offset`) para evitar sobrecarga e perda de dados.
De forma resumida, o código:
- Consome a API do NYC 311;
- Filtra os dados pela data de criação da reclamação;
- Divide o período informado em meses;
- Faz múltiplas requisições paginadas de até 50 mil registros;
- Gera arquivos CSV por mês e por offset;
- Envia automaticamente esses arquivos para um bucket no Google Cloud Storage;
- Verifica se o arquivo já existe no bucket antes de baixar novamente, evitando reprocessamento.
Os arquivos são organizados no GCS seguindo um padrão de nomenclatura que facilita o controle e a leitura posterior no BigQuery, ficando armazenados na pasta `landing_nyc_311`. Essa abordagem garantiu estabilidade no processo de ingestão, além de permitir reprocessamentos parciais caso necessário.


---

## 🏗️ Arquitetura de Medalhão (Camadas no BigQuery)

### 1. Camada Landing
A camada Landing foi criada no BigQuery utilizando uma tabela externa, apontando diretamente para os arquivos CSV armazenados no Google Cloud Storage.
Nesta etapa, os dados permanecem em seu formato original, sem transformações relevantes, servindo apenas como ponto de leitura inicial. As colunas ainda estão em inglês e todos os campos são tratados como texto.
Após a criação da tabela externa, foi realizada uma validação básica da ingestão, verificando:
- A data mínima e máxima de criação das reclamações;
- O total de registros carregados.
Essa validação garante que o período esperado foi corretamente ingerido.


### 2. Camada Raw
A camada Raw tem como foco preparar os dados para tratamento, realizando ajustes estruturais sem alterar o conteúdo original das informações.
Nessa etapa foram feitas principalmente:
- Conversões de tipo (datas para TIMESTAMP, chaves e coordenadas para tipos numéricos);
- Tradução e padronização dos nomes das colunas do inglês para o português;
- Organização do schema de forma mais legível e consistente.
Essa camada preserva a fidelidade dos dados de origem, funcionando como uma versão “bruta tratada”, pronta para receber regras de qualidade.


### 3. Camada Staging
Onde ocorre o "coração" do tratamento:
* **Limpeza:** Substituição de nulos por "Não informado".
* **Padronização:** Formatação de CEPs e textos.
* **Regras de Negócio:** Criação de colunas como `tempo_resolucao_dias` e `categoria_tempo_resolucao`.

A camada Staging é onde acontece a maior parte do tratamento dos dados e a aplicação das regras de negócio, funcionando como um contrato de dados estável entre a engenharia e o consumo analítico.
Nessa etapa, os dados passam por um processo aprofundado de padronização, validação e limpeza, com foco em garantir qualidade, consistência e confiabilidade para as análises. Entre os principais tratamentos realizados estão:
- Padronização de textos (formatação, capitalização e consistência);
- Substituição de valores nulos ou inválidos por “Não informado” quando aplicável;
- Normalização de códigos como N/A, UNKNOWN e Unspecified, também convertidos para “Não informado”;
- Validação e padronização de CEPs, garantindo o formato de até 5 dígitos numéricos;
- Remoção de textos suspeitos ou inconsistentes na coluna tipo_reclamacao;
- Validação de datas inconsistentes, garantindo que o tempo de resolução seja calculado apenas quando a data de fechamento é válida e posterior à data de criação.
Além disso, nesta camada foram criados campos e métricas derivadas fundamentais para a análise, como:
- `status_analise`, que agrupa os diversos status técnicos em categorias mais simples e compreensíveis;
- `tempo_resolucao_dias`, calculando o número de dias entre abertura e fechamento da reclamação;
- `categoria_tempo_resolucao`, que classifica o tempo de resolução das reclamações em rápida, média ou lenta.
A camada Staging é fundamental para centralizar regras de negócio, evitar retrabalho nas camadas analíticas e garantir que as tabelas da camada Trusted sejam construídas a partir de dados já validados e padronizados.


### 4. Camada Trusted
A camada Trusted concentra tabelas já consolidadas e prontas para consumo analítico.
A partir da Staging, foram criadas diversas tabelas específicas para responder perguntas de negócio, como:
- Quais são os tipos de reclamação mais frequentes;
- Quais bairros concentram mais reclamações;
- Qual o status atual das reclamações;
- Quanto tempo, em média, cada reclamação leva para ser resolvida;
- Como as agências se comportam em volume e eficiência;
- Quais agências concentram mais casos classificados como lentos;
- Quais são os principais canais de abertura das reclamações;
- Como o volume de reclamações evolui ao longo do tempo.
Também foi criada uma tabela geral (`trusted_nyc_geral`), que reúne as principais dimensões e fatos do conjunto de dados. Essa tabela funciona como a principal fonte de dados para o Looker Studio, simplificando a construção do dashboard e garantindo uma única fonte de verdade.
A tabela trusted_nyc_geral possui a seguinte estrutura:

#### Estrutura da Tabela `trusted_nyc_geral`
### Estrutura da Tabela `trusted_nyc_geral`

| Coluna | Tipo | Descrição |
|------|------|-----------|
| `chave_unica` | INTEGER | Identificador único da reclamação no sistema NYC 311 |
| `agencia` | STRING | Código da agência responsável pelo atendimento da reclamação |
| `nome_agencia` | STRING | Nome completo da agência responsável pela reclamação |
| `bairro` | STRING | Bairro onde a reclamação foi registrada |
| `tipo_reclamacao` | STRING | Tipo ou categoria da reclamação informada pelo cidadão |
| `status_reclamacao` | STRING | Status atual da reclamação (Resolvido, Em andamento, Cancelado ou Não especificado) |
| `data_criacao` | TIMESTAMP | Data e hora de abertura da reclamação |
| `data_fechamento` | TIMESTAMP | Data e hora de fechamento da reclamação, quando aplicável |
| `ano` | INTEGER | Ano de criação da reclamação, derivado da data de abertura |
| `dias_para_resolver` | INTEGER | Quantidade de dias entre a data de criação e a data de fechamento |
| `categoria_tempo_resolucao` | STRING | Classificação do tempo de resolução (Rápida, Média ou Lenta) |
| `tipo_canal_abertura` | STRING | Canal utilizado para abertura da reclamação (Mobile, Online, Phone ou Não informado) |
*Tabela 1. Estrutura da tabela trusted_nyc_geral*

---

## 📊 Visualização dos Dados
Os dados da camada Trusted são consumidos no Looker Studio, onde foram construído o dashboard para análise de volume de reclamações, desempenho das agências, tempo de resolução e distribuição das reclamações ao longo do tempo e por região.

---

## 🚀 Considerações Finais 
Este projeto demonstra a construção de um pipeline de dados completo, desde a ingestão até a visualização, aplicando boas práticas de Engenharia de Dados e modelagem analítica. A arquitetura adotada é escalável, organizada e preparada para análises consistentes, podendo ser facilmente expandida para novos períodos ou para a inclusão de novas métricas.
Como evolução do projeto, o script de ingestão em Python poderia ser executado por meio de uma ferramenta de orquestração, como o Apache Airflow, reduzindo a dependência de execuções manuais e de ambientes locais, além de aumentar a confiabilidade e a rastreabilidade do processo.
Da mesma forma, a utilização de ferramentas como o dbt permitiria uma organização ainda mais robusta do modelo de dados, com versionamento, documentação das colunas, aplicação de testes de consistência e validações de qualidade, fortalecendo a governança e a manutenção do pipeline ao longo do tempo.

