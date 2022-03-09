from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("E:/Workspaces/pysparkProject/resources/Datanet.csv").coalesce(5)
df.createOrReplaceTempView("dftable")
df.show()
from pyspark.sql.functions import max, min
df.select(max("Weight"), min("Weight")).show() #gives maximum and minimum value of the column
spark.sql("select max(Weight), min(Weight) from dftable").show()
