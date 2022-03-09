from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
from pyspark.sql.functions import lit
df = spark.read.format("csv").option("header", "true").load("E:/Workspaces/pysparkProject/resources/orders.txt")
df.printSchema()
df.select(lit(5), lit("five"), lit(5.0)).show()

