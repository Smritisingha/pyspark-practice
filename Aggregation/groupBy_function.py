from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("E:/Workspaces/pysparkProject/resources/Datanet.csv").coalesce(5)
df.createOrReplaceTempView("dftable")
df.show()
from pyspark.sql.functions import count, expr, stddev_pop
df.groupBy("Name", "Number").count().show()
df.groupBy("Number").agg(count("Weight").alias("weight"),expr("count(Weight)")).show()
df.select(stddev_pop("Number")).show()
df.groupBy("Weight").agg(expr("avg(Number)"),expr("stddev_pop(Number)")).show()