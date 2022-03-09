from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("df").getOrCreate()

from pyspark.sql import Row
from pyspark.sql.types import StringType, StructField, StructType, LongType

myschema = StructType([
    StructField("some",StringType(), True),
    StructField("col",StringType(), True),
    StructField("Name",LongType(), False)
])
myrow = Row("Hello", None, 1)
mydf = spark.createDataFrame([myrow], myschema)
mydf.show()

