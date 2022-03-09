from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("E:/Workspaces/pysparkProject/resources/Datanet.csv").coalesce(5)
df.createOrReplaceTempView("dftable")
df.show()
from pyspark.sql.functions import var_pop, stddev_pop, var_samp, stddev_samp

df.select(var_pop("Number"), var_samp("Number"),stddev_samp("Number"),stddev_pop("Number")).show()
