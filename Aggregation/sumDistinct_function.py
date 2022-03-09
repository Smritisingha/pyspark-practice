from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("E:/Workspaces/pysparkProject/resources/Datanet.csv").coalesce(5)
df.createOrReplaceTempView("dftable")
df.show()
from pyspark.sql.functions import sumDistinct
df.select(sumDistinct("Weight")).show() #gives sum  of distinct sets of value
spark.sql("select sum(Weight) from dftable").show()
