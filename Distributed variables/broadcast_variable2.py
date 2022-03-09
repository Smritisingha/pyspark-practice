from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples').getOrCreate()

df = spark.read.option("delimiter", "|").option("header", "true").csv("C:/Users/smrit/Downloads/population.txt")
lookup = dict(
    {"TX": "Texas",
     "NY": "New York",
     "OH": "OHIO",
     "CA": "Califonia",
     "IL": "Illinois",
     "CO": "Colorado",
     "AZ": "Arizona"})
broad = spark.sparkContext.broadcast(lookup)
broad.value["NY"]

from pyspark.sql import udf
from pyspark.sql.functions import *


def broadval(col):
    return broad.value[col]


funcreg = udf(broadval)

df.withColumn("state", funcreg("State_code")).show()
