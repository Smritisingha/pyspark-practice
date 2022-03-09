from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("E:/Workspaces/pysparkProject/resources/Datanet.csv").coalesce(5)
df.createOrReplaceTempView("dftable")
df.cache()
df.show()
from pyspark.sql.functions import first, last

df.select(first("Number"), last("Number")).show() #gives first and last row of the column
spark.sql("select first(Number), last(Number) from dftable").show()

