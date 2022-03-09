from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("E:/Workspaces/pysparkProject/resources/Datanet.csv")

from pyspark.sql.window import Window
from pyspark.sql.functions import count, avg, col, sum
df.select(count("Number")).show()

windowSpec = Window.partitionBy("Team")
data = df.withColumn("avgsalary",avg(col("Salary")).over(windowSpec))\
    .withColumn("totalsalary",sum(col("Salary")).over(windowSpec))

data.show()

datag = df.groupBy("Team").agg(avg(col("Salary").alias("avgsalary")),
                               sum(col("Salary").alias("totalsalary")))
datag.show()