from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
# creating dataframe
df = spark.read.format("csv").option("header", "true").load("E:/Workspaces/pysparkProject/resources/orders.txt")
df.printSchema()
df.show()
# creating view of df
df.createOrReplaceTempView("dftable")
spark.sql("select * from dftable").show()
