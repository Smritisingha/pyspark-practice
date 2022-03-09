from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
# creating dataframe
from pyspark.sql.functions import monotonically_increasing_id
df = spark.read.format("csv").option("header", "true").load("E:/Workspaces/pysparkProject/resources/orderitms.txt")
df.printSchema()
# this function generates unique value to each row
df.select(monotonically_increasing_id()).show(2)