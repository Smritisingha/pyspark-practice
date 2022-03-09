from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("df").getOrCreate()

df = spark.read.format("csv").load("E:/Workspaces/pysparkProject/resources/Datanet.csv", header = True)
from pyspark.sql.functions import expr, first
# expr("(((somecol + 5) * 200) - 6 )< othercol")

#spark.read.format("csv").load("E:/Workspaces/pysparkProject/resources/Datanet.csv").columns
print(df.first())
print(df.columns)
from pyspark.sql import Row

myrow = Row("Hello", None, 1, False)

print(myrow[0])
