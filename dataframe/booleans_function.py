from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
from pyspark.sql.functions import col

df = spark.read.format("csv").option("header", "true").load("E:/Workspaces/pysparkProject/resources/orders.txt")
df.printSchema()
df.where(col("order_status") != "CLOSED") \
    .select("order_status", "order_cid").show()

df.where("order_id = 4").show(5)

