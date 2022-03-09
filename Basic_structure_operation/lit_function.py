from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("df").getOrCreate()

df = spark.read.format("csv").load("E:/Workspaces/pysparkProject/resources/Datanet.csv", header = True)

from pyspark.sql.functions import lit,expr

df.select(expr("*"), lit(1).alias("one")).show()

# adding column
df.withColumn("numberone", lit(1)).show()

