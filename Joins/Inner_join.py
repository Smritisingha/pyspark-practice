from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('join').getOrCreate()

df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("E:/Workspaces/pysparkProject/resources/orders.txt")
df.show()
df2 = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("E:/Workspaces/pysparkProject/resources/orderitms.txt")
df2.show()

# inner join is the default join in pyspark this joins two dataframes on
# key columns where keys don’t match the rows get dropped from both datasets
df.join(df2,df.order_id == df2.orderitem_order_id,"inner").show()

