from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('join').getOrCreate()

df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("E:/Workspaces/pysparkProject/resources/orders.txt")
df.show()
df2 = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("E:/Workspaces/pysparkProject/resources/orderitms.txt")
df2.show()

# leftsemi join is similar to inner join difference being leftsemi join returns
# all columns from the left dataset and ignores all columns from the right dataset.
# In other words, this join returns columns from the only left dataset for the records
# match in the right dataset on join expression, records not matched on join expression
# are ignored from both left and right datasets.

df.join(df2,df.order_id == df2.orderitem_order_id,"left_semi").show()



