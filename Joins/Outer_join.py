from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('join').getOrCreate()

df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("E:/Workspaces/pysparkProject/resources/orders.txt")
df.show()
df2 = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("E:/Workspaces/pysparkProject/resources/orderitms.txt")
df2.show()

# Outer a.k.a full, fullouter join returns all rows from both datasets,
# where join expression doesn’t match it returns null on respective record columns.

df.join(df2,df.order_id == df2.orderitem_order_id,"Outer").show()
df.join(df2,df.order_id == df2.orderitem_order_id,"full").show()
df.join(df2,df.order_id == df2.orderitem_order_id,"fullouter").show()

# Left a.k.a Leftouter join returns all rows from the left dataset
# regardless of match found on the right dataset when join expression
# doesn’t match, it assigns null for that record and drops records from
# right where match not found.

df.join(df2,df.order_id == df2.orderitem_order_id,"left").show()
df.join(df2,df.order_id == df2.orderitem_order_id,"leftouter").show()

#Right a.k.a Rightouter join is opposite of left join,
# here it returns all rows from the right dataset regardless
# of match found on the left dataset, when join expression doesn’t match,
# it assigns null for that record and drops records from left where match not found.

df.join(df2,df.order_id == df2.orderitem_order_id,"right").show()
df.join(df2,df.order_id == df2.orderitem_order_id,"rightouter").show()

