from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
df = spark.read.option("header", "true").csv("E:/Workspaces/pysparkProject/resources/Datanet.csv")
df.show()

from pyspark.sql.functions import split, col, create_map, explode

df.withColumn("splitted", split(col("Team")," "))\
    .withColumn("exploded", explode(col("splitted")))\
    .select("Team", "splitted", "exploded").show()


df.select(create_map(col("Team"), col("Name")).alias("complexmap")).show() #

df.select(map(col("Team"), col("Name")).alias("complex_map")).selectExpr("complex_map[Boston ]").show()

df.select(map(col("Team"), col("Name")).alias("complex_map"))\
    .selectExpr("explode(complex_map)").show()