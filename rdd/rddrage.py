from pyspark.sql import SparkSession

import findspark

findspark.init()

spark = SparkSession.builder.master("local[*]").getOrCreate()

spark.range(10).toDF("id").rdd.map(lambda row: row[0]).collect()

range_rdd = spark.range(10).rdd

for i in range_rdd.collect():
    print(i)

