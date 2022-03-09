from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
# creating dataframe
df = spark.read.format("csv").option("header", "true").load("E:/Workspaces/pysparkProject/resources/orderitms.txt")
df.printSchema()

df.stat.crosstab("orderitem_order_id", "orderitem_quantity").show()
df.stat.freqItems(["orderitem_order_id", "orderitem_quantity"]).show()

