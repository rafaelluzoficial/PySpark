# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

# Criando um dataframe simples SEM schema
df1 = spark.createDataFrame([
  ("Pedro", 10),
  ("Maria",20),
  ("José",40)
])

df1.show()

# COMMAND ----------

# Criando um dataframe simples COM schema
schema1 = "id INT, Nome STRING"
dados = [
  [1, "Pedro"],[2, "Maria"]
]

df2 = spark.createDataFrame(dados, schema1)
df2.show()

# COMMAND ----------

# Agregação de valores iguais usando GROUPBY e AGG
from pyspark.sql.functions import sum

schema2 = "Produtos STRING, Vendas INT"
dadosVendas = [
  ["Caneta", 10],
  ["Lápis", 20],
  ["Caneta", 40]
]

df3 = spark.createDataFrame(dadosVendas, schema2)
df3.show()

agrupado = df3.groupBy("Produtos").agg(sum("Vendas"))
agrupado.show()

# COMMAND ----------

# Exibindo dados usando comando Select
df3.select("Produtos", "Vendas").show()

# COMMAND ----------

# Criando uma coluna calculada usando EXPR
from pyspark.sql.functions import expr

df3.select("Produtos", "Vendas", expr("Vendas * 0.2")).show()

# COMMAND ----------

# Fazendo a ingestão de arquivo externos
from pyspark.sql.types import *

arqSchema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"
path_dataset = '/FileStore/tables/despachantes.csv'

# Lê arquivos formato CSV
df_despachantes1 = spark.read.csv(path_dataset, header=False, schema=arqSchema)
df_despachantes1.show()

# Lê arquivos de qualquer formato
df_despachantes2 = spark.read.load(path_dataset, header=False, format='csv', sep=',', inferSchema=True)
df_despachantes2.show()

df_despachantesX = spark.read.format('csv').load(path_dataset)
df_despachantesX.show()

# COMMAND ----------


# Criando filtros com WHERE
from pyspark.sql import functions as Func

# Obtendo vendas > 20
df_despachantes1.select('id', 'nome', 'vendas').where(Func.col('vendas') > 20).show()

# Condicional E = &, OU = |, NOT = ~
# Obtendo vendas > 20 & < 40
df_despachantes1.select('id', 'nome', 'vendas').where((Func.col('vendas') > 20) & (Func.col('vendas') < 40)).show()

# COMMAND ----------

# Renomeando colunas de um dataframe
df4 = df_despachantes1.withColumnRenamed('nome', 'clientes')
df4.columns

# COMMAND ----------

# Criando nova coluna com novo tipo de dados
from pyspark.sql.functions import *

# Nova coluna nova_data (timestamp) baseada na coluna data (string)
df5 = df_despachantes1.withColumn('nova_data', to_timestamp(Func.col('data'), 'yyyy-MM-dd'))
df5.schema

# COMMAND ----------

# Manipulando dados
df5.show()

# Exibindo apenas o ano nas colunas de data
df5.select('nome', year('data')).show()

# COMMAND ----------

# Manipulando dados
df5.show()

# Exibindo datas únicas
df5.select(year('nova_data')).distinct().show()

# COMMAND ----------

# Manipulando dados com ORDERBY
df5.show()

# Exibindo colunas ordenadas
df5.select('nome', year('nova_data')).orderBy('nome').show()
df5.orderBy(Func.col('cidade').desc(), Func.col('vendas').desc()).show()

# COMMAND ----------

# Manipulando dados com GROUPBY
df5.show()

# Exibindo quantidade de vendas por cidade
df5.groupBy('cidade').agg(sum('vendas')).show()

# Ordenando o resultado com novo nome da coluna
df5.groupBy('cidade').agg(sum('vendas')).orderBy(Func.col('sum(vendas)')).show()

# COMMAND ----------

# Manipulando dados
df5.show()

# Contando elementos únicos com COUNT
df5.select('data').groupBy(year('data')).count().show()

# COMMAND ----------

# Manipulando dados
df5.show()

# Somando quantidade de registros com SUM
df5.select(Func.sum('vendas')).show()

# COMMAND ----------

# Manipulando dados
df5.show()

# Retorna o número de linhas do dataframe
df5.count()

# COMMAND ----------

# Manipulando dados
df5.show()

# Filtrando dados com FILTER
df5.filter(Func.col('nome') == 'Deolinda Vilela').show()

# COMMAND ----------

# Salvando dataframe em disco
path_destino="/FileStore/tables/PARQUET/"
nome_arquivo="despachante.parquet"
path_geral= path_destino + nome_arquivo
df5.write.format("parquet").mode("overwrite").save(path_geral)
