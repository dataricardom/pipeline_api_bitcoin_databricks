# Databricks notebook source
#Importando bibliotecas nescessárias.

import requests
import pandas as pd
from datetime import datetime

# COMMAND ----------

#Criando parametros para atribuir a variavel.
dbutils.widgets.text("api_key", "", "API Key CurrencyFreaks")


# COMMAND ----------

#Extraindo dados da API Coinbase de valor de Bitcoin.

def extrair_dados_bitcoin():
    url = 'https://api.coinbase.com/v2/prices/spot'
    resultado = requests.get(url)
    return resultado.json()



# COMMAND ----------

#Adicionando dados a uma variável vindos da API coinbase 

dados_bitcoin = extrair_dados_bitcoin()

# COMMAND ----------

#Extraindo dados da API currencyfreaks.

def extrair_dados_currentfreaks():
    api_key = dbutils.widgets.get("api_key")
    url = f'https://api.currencyfreaks.com/v2.0/rates/latest?apikey={api_key}'
    resultado = requests.get(url)
    return resultado.json()


# COMMAND ----------

#Adicionando dados a uma variável vindos da API currencyfreaks.

dados_cotacao = extrair_dados_currentfreaks()

#Pegando somente uma parte dos dados vindos da API e convertendo para FLOAT.

cotacao_br=float(dados_cotacao['rates']['BRL'])


# COMMAND ----------

#Fazendo tratamentos dos dados vindos das API's e atribuindo a dados_tratados.

def tratar_dados_bitcoin(dados_bitcoin,cotacao_br):
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

# COMMAND ----------

tratar_dados_bitcoin(dados_bitcoin, cotacao_br)

# COMMAND ----------

#Adicionando dados extraidos e tratados a um Dataframe.

df_dados_bitcoin =(tratar_dados_bitcoin(dados_bitcoin, cotacao_br))



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Criando um Catalogo para armazenamentos dos dados 
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS catalogo_bitcoin;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Criando SCHEMA para armazenamendo dos dados no catalogo criado.
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS catalogo_bitcoin.data_bitcoin
# MAGIC

# COMMAND ----------

#Criando Dataframe com os dados tratatos df_dados_bitcoin.

df = spark.createDataFrame(df_dados_bitcoin)

# COMMAND ----------

#Criando um path com o caminho do catalogo, schema e nome da tabela.

create_table_delta_name = 'catalogo_bitcoin.data_bitcoin.bitcoin_data'

# COMMAND ----------

#Criando e salvando em formato delta a tabela, no catálogo e schema definidos junto ao nome.

(df.write
.format("delta")
.mode("append")
.option("mergeSchema", "True")
.saveAsTable(create_table_delta_name)
)
