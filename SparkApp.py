import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Para executar o app, usaremos o Spark Submit com o comando
# spark-submit [nome do programa].py [caminho do arquivo de dados]

if __name__ == '__main__':
    # inicia sessão spark
    spark = SparkSession.builder.appName("Exemplo").getOrCreate()

    # cria schema dos dados para ingestão
    arqSchema = "id INT, nome STRING, status STRING, cidade STRING, vendas INT, data STRING"

    # cria df com dados do arquivo csv
    # despachantes = spark.read.csv("/home/rafael/download/despachantes.csv", header=False, schema = arqSchema)
    despachantes = spark.read.csv(sys.argv[1], header=False, schema=arqSchema)

    # realiza manipulação dos dados
    calculo = despachantes.select("data").groupBy(year("data")).count()

    # imprime dados na tela do console
    calculo.write.format("console").save()

    # encerra sessão spark
    spark.stop()