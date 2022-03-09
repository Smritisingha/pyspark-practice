from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("E:/Workspaces/pysparkProject/resources/Datanet.csv").coalesce(5)
df.createOrReplaceTempView("dftable")
df.cache()
df.show()
from pyspark.sql.functions import approxCountDistinct
df.select(approxCountDistinct("Number", 0.1)).show()
spark.sql("select approx_Count_distinct('Number', 0.1) from dftable").show()
