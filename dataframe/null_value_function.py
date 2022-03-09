from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
from pyspark.sql.functions import col
df = spark.read.option("header", "true").csv("E:/Workspaces/pysparkProject/resources/fornull.csv")
df.show()
df.printSchema()
df1 = df.withColumn("Salary", df.Salary.cast("long")).withColumn("Age", df.Age.cast("int")) # convert salary integer to long
df1.printSchema()
df.na.drop("all", subset=["Team", "College","Salary"]).show()  # drop function removes rows that contain nulls.

df.na.fill("fill all").show()  # fill function can fill one or more num columns with a sets of values

df.na.replace([" "], ["77"], "Age").show() #replace all values in a certain column according to their corrent values

df.na.fill("all", subset=["Team", "College","Salary"]).show() # fill with perticular value in mention column

fill_col_vals = {"Number": 5, "Team": "no value", "Age": 90 }
df.na.fill(fill_col_vals).show()

df1.na.fill(545611).show(5) #we can give string or int value to fill null for particular column whose value is null

