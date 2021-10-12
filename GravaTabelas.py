import sys, getopt
from pyspark.sql import SparkSession

# Para executar o app, usaremos o Spark Submit com o comando
# spark-submit --jars [caminho do driver odbc.jar] [nome do programa].py [parâmetros]

if __name__ == '__main__':
    # inicia sessão spark
    spark = SparkSession.builder.appName("GravaTabelas").getOrCreate()

    # recupera parametros passados na linha de comando
    # t = formato do arquivo de entrada
    # i = caminho do arquivo de entrada
    # n = nome da tabela a ser criada no bd
    opts, args = getopt.getopt(sys.argv[1:], "t:i:n:")
    formato, infile, name = "", "", ""

    for opt, arg in opts:
        if opt == "-t":
            formato = arg
        elif opt == "-i":
            infile = arg
        elif opt == "-n":
            name = arg

    dados = spark.read.load(infile)
    dados.write.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/tabelas").option("dbtable", name).option("user", "postgres").option("password", "123456").option("driver", "org.postgresql.Driver").save()

    # encerra sessão spark
    spark.stop()