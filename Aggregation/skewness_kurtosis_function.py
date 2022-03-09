from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("E:/Workspaces/pysparkProject/resources/Datanet.csv").coalesce(5)
df.createOrReplaceTempView("dftable")
df.show()
from pyspark.sql.functions import skewness, kurtosis, corr, covar_pop, covar_samp
df.select(skewness("Number"), kurtosis("Number")).show()
df.select(corr("Number", "Weight"), covar_pop("Number", "Weight"), covar_samp("Number", "Weight")).show()

