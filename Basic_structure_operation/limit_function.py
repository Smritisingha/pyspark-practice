from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("df").getOrCreate()

df = spark.read.format("csv").load("E:/Workspaces/pysparkProject/resources/Datanet.csv", header = True)
from pyspark.sql.functions import col, asc,desc, expr
df.limit(5).show()
df.orderBy(expr("Salary desc")).limit(5).show()

print(df.rdd.getNumPartitions()) # 1 # get the partition number
df1 = df.repartition(5) # repartition 5
print(df1.rdd.getNumPartitions()) #5