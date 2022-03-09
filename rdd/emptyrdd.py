from pyspark.sql import SparkSession

spark: SparkSession = SparkSession.builder.master("local[1]").appName("Spark").getOrCreate()

rdd = spark.sparkContext.emptyRDD() # creating emptyrdd
print(rdd.collect())
