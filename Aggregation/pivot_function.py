from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('agg').getOrCreate()

df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("E:/Workspaces/pysparkProject/resources/Datanet.csv")
df.createOrReplaceTempView("dftable")
df.show()

#pivot() function to rotate the data from one column into multiple columns.
df.groupBy("Number").pivot("Position").sum().show()
pivotdf = df.groupBy("Number").pivot("Position").sum("Salary")
pivotdf.show()

df.groupBy("Age").pivot("Team").sum("Salary").show()
