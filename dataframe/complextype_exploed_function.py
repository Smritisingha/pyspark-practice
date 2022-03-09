from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
df = spark.read.option("header", "true").csv("E:/Workspaces/pysparkProject/resources/Datanet.csv")
df.show()

from pyspark.sql.functions import split, col, explode

df.withColumn("splitted", split(col("Team")," "))\
    .withColumn("exploded", explode(col("splitted")))\
    .select("Team", "splitted", "exploded").show()

# splitted  split the column Team , exploded explode(first splited value in one row 2nd splited value in another row) the column splitted


