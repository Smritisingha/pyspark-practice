from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
# creating dataframe
df = spark.read.format("csv").option("header", "true").load("E:/Workspaces/pysparkProject/resources/orderitms.txt")
df.printSchema()
# 2.0 is power
df.selectExpr("orderitem_id", "(POWER((orderitem_product_id * orderitem_quantity), 2.0) + 5)as realquantity").show()
