from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
df = spark.read.option("header", "true").csv("E:/Workspaces/pysparkProject/resources/Datanet.csv")
df.show()
from pyspark.sql.functions import struct, col
complexdf = df.select(struct("Name", "Team").alias("complex")) # merge the columns Name and Team
complexdf.createOrReplaceTempView("complexDF") # create tempview
complexdf.show()
complexdf.select("complex.Name").show() # dot syntax gives one column only
complexdf.select(col("complex").getField("Team")).show() # dot getField gives one column only as dot syntax




