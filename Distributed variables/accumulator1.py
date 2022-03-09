from pyspark.sql import SparkSession

spark: SparkSession = SparkSession.builder.master("local[1]").appName("Spark").getOrCreate()

num = spark.sparkContext.accumulator(10)
print(num)

def f(x):
    global num
    num += x


rdd = spark.sparkContext.parallelize([20, 30, 40, 50])
rdd.foreach(f)
final = num.value
print(final)
