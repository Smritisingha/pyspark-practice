from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()

from  pyspark.sql.functions import regexp_replace , col
df = spark.read.format("csv").option("header", "true").load("E:/Workspaces/pysparkProject/resources/Datanet.csv")
regex_string = "Boston"
df.select(regexp_replace(col("Team"), regex_string, "mr").alias("replace"),col("Team")).show()
# Boston replace with mr

from  pyspark.sql.functions import translate
df.select(translate(col("Team"), "absg", "1478"), col("Team")).show()
# absg translate with 1478

from  pyspark.sql.functions import regexp_extract

extr_string = "(Boston|Brooklyn)"
df.select(regexp_extract(col("Team"), extr_string, 1).alias("extract"), col("Team")).show()
# extract only 1 

