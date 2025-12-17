# 📊 Ingestão de Dados de Bitcoin no Databricks

Este projeto demonstra, o passo a passo de um **pipeline simples de ingestão de dados** utilizando **Databricks**, consumindo dados de APIs externas, tratando as informações e persistindo os resultados em uma **tabela Delta Lake**. Ao final, o notebook é executado de forma **automatizada e agendada** por meio de um **Databricks Job**.

---

## 🧠 Visão Geral do Processo

O fluxo executado no Databricks segue as etapas abaixo:

1. Importação das bibliotecas necessárias
2. Criação de um parâmetro (widget) para receber a API Key
3. Extração de dados da API do Coinbase (Bitcoin)
4. Extração da taxa de câmbio USD → BRL via API CurrencyFreaks
5. Tratamento e enriquecimento dos dados
6. Criação de catálogo e schema no Databricks
7. Criação de um DataFrame Spark
8. Persistência dos dados em uma tabela Delta
9. Criação de um Job para execução agendada do notebook

---

## 1️⃣ Importação das bibliotecas

Inicialmente, são importadas as bibliotecas necessárias para requisições HTTP, manipulação de dados e geração de timestamps.

```python
import requests
import pandas as pd
from datetime import datetime
```

---

## 2️⃣ Criação de parâmetro (Widget)

No Databricks, foi criado um **widget de texto** para receber a **API Key da CurrencyFreaks** dinamicamente, sem necessidade de alterar o código.

```python
dbutils.widgets.text("api_key", "", "API Key CurrencyFreaks")
```

📌 **Objetivo:** permitir que o valor da API Key seja informado manualmente ou passado automaticamente por um Job.

---

## 3️⃣ Extração de dados da API Coinbase

Foi criada uma função responsável por buscar o valor atual do Bitcoin em USD a partir da API pública do Coinbase.

```python
def extrair_dados_bitcoin():
    url = 'https://api.coinbase.com/v2/prices/spot'
    resultado = requests.get(url)
    return resultado.json()
```

Em seguida, os dados são atribuídos a uma variável:

```python
dados_bitcoin = extrair_dados_bitcoin()
```

---

## 4️⃣ Extração da taxa de câmbio (CurrencyFreaks)

Outra função foi criada para consumir a API da **CurrencyFreaks**, utilizando a API Key informada no widget.

```python
def extrair_dados_currentfreaks():
    api_key = dbutils.widgets.get("api_key")
    url = f'https://api.currencyfreaks.com/v2.0/rates/latest?apikey={api_key}'
    resultado = requests.get(url)
    return resultado.json()
```

Após a extração, é obtida apenas a taxa de conversão de USD para BRL:

```python
dados_cotacao = extrair_dados_currentfreaks()
cotacao_br = float(dados_cotacao['rates']['BRL'])
```

---

## 5️⃣ Tratamento e enriquecimento dos dados

Nesta etapa, os dados das duas APIs são combinados e tratados em uma única estrutura, incluindo:

* Valor do Bitcoin em USD
* Moeda base
* Moeda de origem
* Conversão para BRL
* Timestamp da extração

```python
def tratar_dados_bitcoin(dados_bitcoin, cotacao_br):
    valor_usd = float(dados_bitcoin['data']['amount'])
    criptomoeada = dados_bitcoin['data']['base']
    moeada_orig = dados_bitcoin['data']['currency']

    valor_br1 = valor_usd * cotacao_br
    timestamp = datetime.now()

    dados_tratados = [{
        "valor_usd": valor_usd,
        "criptomoeada": criptomoeada,
        "moeada_orig": moeada_orig,
        "taxa_conversao_usd_to_brl": valor_br1,
        "timestamp": timestamp
    }]
    return dados_tratados
```

---

## 6️⃣ Criação do DataFrame Spark

Os dados tratados são convertidos para um **DataFrame Spark**, permitindo integração com o Delta Lake.

```python
df_dados_bitcoin = tratar_dados_bitcoin(dados_bitcoin, cotacao_br)
df = spark.createDataFrame(df_dados_bitcoin)
```

---

## 7️⃣ Criação do Catálogo e Schema

Utilizando comandos SQL no Databricks, foi criado um **catálogo** e um **schema** para organização dos dados.

```sql
CREATE CATALOG IF NOT EXISTS catalogo_bitcoin;
```

```sql
CREATE SCHEMA IF NOT EXISTS catalogo_bitcoin.data_bitcoin;
```

---

## 8️⃣ Persistência dos dados em Delta Lake

Os dados são gravados em uma **tabela Delta**, permitindo escalabilidade, versionamento e integração com outros pipelines.

```python
create_table_delta_name = 'catalogo_bitcoin.data_bitcoin.bitcoin_data'

(df.write
  .format("delta")
  .mode("append")
  .option("mergeSchema", "True")
  .saveAsTable(create_table_delta_name)
)
```

📌 **Observação:** o modo `append` permite inserir novos registros a cada execução do pipeline.

---

## 9️⃣ Criação de Job e execução agendada

Por fim, foi criado um **Databricks Job** para executar este notebook de forma **automática e agendada**, sem intervenção manual.

### ✔ O Job permite:

* Executar o notebook em intervalos definidos (ex: a cada 5 ou 20 minutos)
* Passar o valor da `api_key` como parâmetro
* Automatizar completamente o processo de ingestão

📅 O agendamento foi configurado utilizando **cron no padrão Databricks (Quartz)**.

---

## ✅ Conclusão

Este notebook demonstra um fluxo completo de:

* Consumo de APIs externas
* Parametrização no Databricks
* Tratamento de dados
* Persistência em Delta Lake
* Automação com Jobs agendados

Tudo isso foi realizado **inteiramente dentro do Databricks**, seguindo boas práticas de engenharia de dados.
