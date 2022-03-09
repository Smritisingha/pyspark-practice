from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
from pyspark.sql.functions import bround, round, lit, corr
from pyspark.sql.types import IntegerType, FloatType


orderscsv = spark.read.csv("E:/Workspaces/pysparkProject/resources/orders.txt").toDF('order_id','order_d','order_cid','order_status')
df = orderscsv.withColumn('order_id', orderscsv.order_id.cast(IntegerType())).\
    withColumn('order_cid', orderscsv.order_cid.cast(IntegerType()))

df.printSchema()
df.select(round(lit("2.5")), bround(lit("2.5"))).show()

df.stat.corr("order_cid", "order_id")
df.select(corr("order_cid","order_id")).show()

df.describe().show()
