# Como utilizar Spark no Jupyter Notebook
## Instalando requisitos de sistema
### Instalando Jupyter

COMMAND
pip install jupyter
pip install findspark
jupyter notebook

### Dentro do notebook devemos importar as bibliotecas

COMMAND
import findspark
findspark.init()
import pyspark
