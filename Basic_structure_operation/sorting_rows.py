from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("df").getOrCreate()

df = spark.read.format("csv").load("E:/Workspaces/pysparkProject/resources/Datanet.csv", header = True)
from pyspark.sql.functions import col, asc,desc, expr
df.sort("Salary").show()
df.orderBy("Salary", "Name").show()
df.orderBy(col("Salary"), col("Name")).show()

# we can use asc, desc in deferent ways
df.orderBy(expr("Salary desc")).show()
df.orderBy(col("Salary").desc(), col("Name").asc()).show()

spark.read.format("csv").load("E:/Workspaces/pysparkProject/resources/Datanet.csv", header = True).sortWithinPartitions("Age").show()