from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()

jsondf = spark.range(1).selectExpr(""" '{"myjsonkey" : {"myjsonvalue" : [1, 2, 3]}}' as jsonstring""")
jsondf.show()

from pyspark.sql.functions import get_json_object, json_tuple, col

jsondf.select(get_json_object(col("jsonstring"), "$.myjsonkey.myjsonvalue[2]").alias("column"),json_tuple(col("jsonstring"), "myjsonkey")).show()

# you can use the get_json_object to inline query a json object
# you can use json_tupple if this object has only one level of nesting 