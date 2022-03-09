from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("df").getOrCreate()

df = spark.read.format("json").load("C:/Users/smrit/Downloads/input.json")
df.printSchema()
# Schema defines the column names and type of dataframe

df = spark.read.format("json").load("C:/Users/smrit/Downloads/input.json").schema
