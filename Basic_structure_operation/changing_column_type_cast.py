from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("df").getOrCreate()

df = spark.read.format("csv").load("E:/Workspaces/pysparkProject/resources/Datanet.csv", header = True)

from pyspark.sql.types import DoubleType, FloatType, LongType, StringType
from pyspark.sql.functions import col
df.printSchema()

df1 = df.withColumn("Salary", col("Salary").cast("long")) # convert salary integer to long
df1.printSchema()