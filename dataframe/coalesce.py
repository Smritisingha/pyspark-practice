from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()

#its allow you to select the first non null value from a sets of column if there are no null values then its returns the first column

df = spark.read.format("csv").option("header", "true").load("E:/Workspaces/pysparkProject/resources/Datanet.csv")
from pyspark.sql.functions import coalesce, col
df.select(coalesce(col("Name"),col("Position"))).show()