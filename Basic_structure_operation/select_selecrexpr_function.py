from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("df").getOrCreate()

df = spark.read.format("csv").load("E:/Workspaces/pysparkProject/resources/Datanet.csv", header = True)

df.select("Name").show()
df.select("Name","Team").show() # you can select multiple columns by using the same style of query

from pyspark.sql.functions import expr, col, column

# you can defined column in manny ways

df.select(
    expr("Name"),
    col("Team"),
    column("Salary")
).show()

df.select(expr("Name as name")).show()

df.select(expr("Name").alias("Names")).show()

df.selectExpr("Name","Salary as emp_salary").show()

df.selectExpr("*","(Name = Team) as result").show()

df.selectExpr("avg(Salary)", "count(distinct(Team))").show()


