from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
from pyspark.sql.functions import to_json, col, from_json
df = spark.read.option("header", "true").csv("E:/Workspaces/pysparkProject/resources/Datanet.csv")
df.show()

df.selectExpr("(Name, Team) as mystruct")\
    .select(to_json(col("mystruct"))).show()
#you can turn a structtype into a json string by using to_json fuction
#from_json to parse json data
from pyspark.sql.types import StructType,StringType,StructField

parseschema = StructType((
    StructField("Team", StringType(),True),
    StructField("Name", StringType(),True)))

df.selectExpr("(Name, Team) as  mystruct")\
    .select(to_json(col("mystruct")).alias("newjson"))\
    .select(from_json(col("newjson"), parseschema), col("newjson")).show()

