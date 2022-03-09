from pyspark.sql import SparkSession

spark: SparkSession = SparkSession.builder.appName("df").getOrCreate()

columns = ["language", "users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]


rdd = spark.sparkContext.parallelize(data)

# to converting rdd to df we used toDF()
dfFromRDD1 = rdd.toDF(columns)
dfFromRDD1.printSchema()

print(dfFromRDD1)

#dfFromRDD1 = rdd.toDF(columns)
#dfFromRDD1.printSchema()

# converting rdd to df by createDataFrame()
dfFromRDD2 = spark.createDataFrame(rdd).toDF(*columns)
print(dfFromRDD2)
