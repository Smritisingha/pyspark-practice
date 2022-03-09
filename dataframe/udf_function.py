from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()

df = spark.range(5).toDF("num")
df.show()

def power3(double_value):
    return double_value **3
print(power3(2.0))

from pyspark.sql.functions import udf, col

power3udf = udf(power3)
df.select(power3(col("num"))).show()

from pyspark.sql.types import IntegerType, DoubleType
spark.udf.register("power3py", power3, DoubleType())

df.selectExpr("power3py(num)").show()


