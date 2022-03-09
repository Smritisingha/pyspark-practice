from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("E:/Workspaces/pysparkProject/resources/Datanet.csv").coalesce(5)
df.createOrReplaceTempView("dftable")
df.cache()
df.show()
from pyspark.sql.functions import count
df.count()
df.select(count("Number")).show() #counts number of rows
spark.sql("select count(*) from dftable").show() #counts every row


