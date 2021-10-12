# Databricks notebook source
# Manipulando dados via PostgreSQL
# Instale o PostgreSQL
# Faça o download do driver JDBC .jar

# Inicie o pyspark apontando para o driver .jar que foi baixado
# pyspark --jars [caminho + nome do jar]

# Carregando dados do BD em um dataframe
# df_vendas = spark.read.format("jdbc").option("url","jdbc:postgresql://localhost:5432/vendas").option("dbtable","Vendas").option("user","postgres").option("password","123456").option("driver","org.postgresql.Driver").load()

# Salvando dados de um DF no BD
# vendadata.write.format("jdbc").option("url","jdbc:postgresql://localhost:5432/vendas").option("dbtable","VendaData").option("user","postgres").option("password","123456").option("driver","org.postgresql.Driver").save()

# COMMAND ----------

# Manipulando dados via MongoDB

# Usar app MongoImport para inportação de dados
# mongoimport --db [nome do bd] --collection [nome da coleção] --legacy --file [caminho do arquivo]

# Verificar no mongodb se o arquivo de dados foi importado
# iniciar no console o mongodb
# sudo systemctl start mongod
# mongosh
# show dbs

# Apontar para o bd importado
# use [nome do bd]

# Verificar todos os documentos importados
# db.[nome da coleção].find()

# Instalando pacote Spark para acessar o mongodb
# Em um novo console digite o comando
# pyspark --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1

# Criando df com dados do mongodb
# posts = spark.read.format("mongo").option("uri","mongodb://localhost/posts.post").load()
# posts.show()

# Salvando dados no mongodb
# posts.write.format("mongo").option("uri","mongodb://localhost/posts2.post").save()
