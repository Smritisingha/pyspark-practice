from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
# creating dataframe
df = spark.read.format("csv").option("header", "true").load("E:/Workspaces/pysparkProject/resources/Datanet.csv")
df.show()
from pyspark.sql.functions import initcap, col, lower, upper
# initcap function will capitalize every word in a given string when that word is separated from another by space

df.select(initcap(col("Name"))).show()

df.select(col("Name"),
          lower(col("Name")),
          upper(lower(col("Name")))).show(7)
