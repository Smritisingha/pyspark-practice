from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('agg').getOrCreate()

df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("E:/Workspaces/pysparkProject/resources/Datanet.csv")
df.createOrReplaceTempView("dftable")
df.show()
from pyspark.sql.functions import sum, col
#rollups is a multidimensional aggregation that performs a variety of groupBy style calculation

df.rollup("Number", "Weight").agg(sum("Salary")).selectExpr("Number","Weight","sum(Salary)").orderBy("Number").show()


df.cube("Number", "Weight").agg(sum(col("Salary"))).selectExpr("Number","Weight","sum(Salary)").orderBy("Number").show()