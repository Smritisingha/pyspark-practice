from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("E:/Workspaces/pysparkProject/resources/Datanet.csv").coalesce(5)
df.createOrReplaceTempView("dftable")
df.show()
from pyspark.sql.functions import avg,count,sum,expr

df.select(avg("Number").alias("avgnumber")).show()
df.select(count("Number").alias("totalnumber"),
          sum("Number").alias("sumnumber"),
          avg("Number").alias("avgNumber"),
          expr("mean(Number)").alias("meannumber"))\
    .selectExpr("sumnumber/totalnumber",
                "avgNumber","meannumber").show()

