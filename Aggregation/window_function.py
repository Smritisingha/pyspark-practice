from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("E:/Workspaces/pysparkProject/resources/Datanet.csv")
df.createOrReplaceTempView("dftable")
df.show()

df.printSchema()

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, max, col

# row_number
windowSpec = Window.partitionBy("Age").orderBy("Salary")
df.withColumn("row_number", row_number().over(windowSpec)).show()

maxsalary = max(col("Salary")).over(windowSpec)

# rank() window function is used to provide a rank to the result within a window partition
from pyspark.sql.functions import rank
df.withColumn("rank", rank().over(windowSpec)).show()

# dens_rank() window function is used to get the result with rank of rows within a window partition without any gaps.
from pyspark.sql.functions import dense_rank
df.withColumn("dense_rank",dense_rank().over(windowSpec)).show()


# percent_rank
from pyspark.sql.functions import percent_rank
df.withColumn("percent_rank",percent_rank().over(windowSpec)).show()

# ntile() window function returns the relative rank of result rows within a window partition.it returns ranking between 2 values (1 and 2)

from pyspark.sql.functions import ntile
df.withColumn("ntile",ntile(2).over(windowSpec)).show()

denserank = dense_rank().over(windowSpec)
rankk = rank().over(windowSpec)

df.where("Number IS NOT NULL").orderBy("Number")\
    .select(col("Number"),col("Salary"),col("Team"),rankk.alias("rank"),denserank.alias("denserank"),maxsalary.alias("maxsalary")).show()

# window function is used to get the cumulative distribution of values within a window partition.
from pyspark.sql.functions import cume_dist
df.withColumn("cume_dist",cume_dist().over(windowSpec)).show()

"""lag"""
from pyspark.sql.functions import lag
df.withColumn("lag",lag("Salary",2).over(windowSpec)).show()




