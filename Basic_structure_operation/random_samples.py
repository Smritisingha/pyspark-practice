from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("df").getOrCreate()

df = spark.read.format("csv").load("E:/Workspaces/pysparkProject/resources/Datanet.csv", header = True)

seed = 5
withReplacement = False
fraction = 0.5
print(df.sample(withReplacement, fraction, seed).count())

# to sample some random records from your dataframe . your can do this by sample method