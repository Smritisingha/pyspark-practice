from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("df").getOrCreate()

from pyspark.sql.types import StringType, StructField, StructType, LongType

myschema = StructType([
    StructField("name",StringType(), True),
    StructField("Age",LongType(), True)
])

df = spark.read.format("json").schema(myschema).load("C:/Users/smrit/Downloads/input.json")
df.printSchema()

from pyspark.sql.functions import col, column
df.col("Age").show()
