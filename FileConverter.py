import sys, getopt
from pyspark.sql import SparkSession

# Para executar o app, usaremos o Spark Submit com o comando
# spark-submit [nome do programa].py [caminho do arquivo de dados]

if __name__ == '__main__':
    # inicia sessão spark
    spark = SparkSession.builder.appName("Exemplo").getOrCreate()

    # recupera parametros passados na linha de comando
    # t = formato do arquivo de entrada
    # i = caminho do arquivo de entrada
    # o = caminho onde será salvo arquivo de saída
    opts, args = getopt.getopt(sys.argv[1:], "t:i:o:")
    formato, infile, outdir = "", "", ""

    for opt, arg in opts:
        if opt == "-t":
            formato = arg
        elif opt == "-i":
            infile = arg
        elif opt == "-o":
            outdir = arg

    # cria df com dados do arquivo csv
    dados = spark.read.csv(infile, header=False, inferSchema=True)
    dados.write.format(formato).save(outdir)

    # encerra sessão spark
    spark.stop()