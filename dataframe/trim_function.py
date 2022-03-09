from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
# creating dataframe
from pyspark.sql.functions import monotonically_increasing_id
df = spark.read.format("csv").option("header", "true").load("E:/Workspaces/pysparkProject/resources/orderitms.txt")
df.printSchema()
from pyspark.sql.functions import ltrim ,rtrim, rpad, lpad, trim , lit

df.select(ltrim(lit("    Hello   ")).alias("ltrim"),
          rtrim(lit("    Hello    ")).alias("rtrim"),
          trim(lit("   Hello    ")).alias("trim"),
          lpad(lit("Hello"), 3, "*").alias("lpad"),
          rpad(lit("Hello"), 10, "*").alias("rpad")).show()

