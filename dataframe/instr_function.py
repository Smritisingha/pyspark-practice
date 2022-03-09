from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()

df = spark.read.format("csv").load("E:/Workspaces/pysparkProject/resources/Datanet.csv", header = True)
df.printSchema()
# orderscsv = spark.read.csv("E:/Workspaces/pysparkProject/resources/orders.txt").toDF('order_id','order_d','order_cid','order_status')
#df = orderscsv.withColumn('order_id', orderscsv.order_id.cast(IntegerType())).\
 #   withColumn('order_cid', orderscsv.order_cid.cast(IntegerType()))

from pyspark.sql.functions import instr
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, FloatType



idfilter = instr(col("Team"), "Boston") >= 1
statusfilter = instr(col("Team"), "Celtics") >= 1
df.withColumn("new", idfilter | statusfilter).where("new").select("Team").show(3)
