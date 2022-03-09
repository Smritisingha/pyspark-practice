from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('join').getOrCreate()

df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("E:/Workspaces/pysparkProject/resources/orders.txt")
df.show()
df2 = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("E:/Workspaces/pysparkProject/resources/orderitms.txt")
df2.show()

# leftanti join does the exact opposite of the leftsemi,
# leftanti join returns only columns from the left dataset for non-matched records.

df.join(df2,df.order_id == df2.orderitem_order_id,"left_anti").show()

