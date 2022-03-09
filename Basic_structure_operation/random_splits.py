from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("df").getOrCreate()

df = spark.read.format("csv").load("E:/Workspaces/pysparkProject/resources/Datanet.csv", header = True)

# Random splits can helps you when you need to breakup your dataframe into a random "splits" of to your original dataframe
seed = 5
withReplacement = False
fraction = 0.5

data = df.randomSplit([0.25, 0.75],seed)
print(data[0].count() > data[1].count())