from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('join').getOrCreate()

df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("E:/Workspaces/pysparkProject/resources/orders.txt")
df.show()
df2 = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("E:/Workspaces/pysparkProject/resources/orderitms.txt")
df2.show()

# crossjoin will join every single row in left dataframe to every single row in right dataframe

df.join(df2,df.order_id == df2.orderitem_order_id,"cross").show()

