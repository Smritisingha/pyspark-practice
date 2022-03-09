from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("df").getOrCreate()

df = spark.read.format("csv").load("E:/Workspaces/pysparkProject/resources/Datanet.csv", header = True)

from pyspark.sql.functions import expr

new = df.withColumn("new",expr("Name"))
new.show()

new.selectExpr("`new`","`new` as `newcol`").show()

print(new.select(expr("`new`")).columns)