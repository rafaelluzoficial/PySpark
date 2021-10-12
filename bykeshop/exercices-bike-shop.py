# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

# Ingestão de arquivos
from pyspark.sql.types import *

path_df_clientes = '/FileStore/tables/Clientes.parquet'
path_df_itens = '/FileStore/tables/ItensVendas.parquet'
path_df_produtos = '/FileStore/tables/Produtos.parquet'
path_df_vendas = '/FileStore/tables/Vendas.parquet'
path_df_vendedores = '/FileStore/tables/Vendedores.parquet'

df_clientes = spark.read.format('parquet').load(path_df_clientes)
df_itens = spark.read.format('parquet').load(path_df_itens)
df_produtos = spark.read.format('parquet').load(path_df_produtos)
df_vendas = spark.read.format('parquet').load(path_df_vendas)
df_vendedores = spark.read.format('parquet').load(path_df_vendedores)

df_clientes.show(10)
df_itens.show(10)
df_produtos.show(10)
df_vendas.show(10)
df_vendedores.show(10)

# COMMAND ----------

# Consulta que mostre Clientes colunas Nome, Estados e Status
df_clientes.select('Cliente', 'Estado', 'Status').show(10)

# COMMAND ----------

# Consulta que mostre apenas clientes com status Platinum e Gold
from pyspark.sql import functions as Func

df_clientes.select('Cliente', 'Estado', 'Status').where((Func.col('Status') == 'Gold') | (Func.col('Status') == 'Platinum')).show()

# COMMAND ----------

# Consulta que mostre quanto cada status de cliente representa em vendas
from pyspark.sql.functions import sum as SUM

df_vendas.join(df_clientes, df_vendas.ClienteID == df_clientes.ClienteID).groupBy(df_clientes.Status).agg(SUM("Total")).orderBy(Func.col("sum(Total)").desc()).show()
