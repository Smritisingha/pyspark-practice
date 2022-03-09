from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()

df = spark.read.format("csv").load("E:/Workspaces/pysparkProject/resources/Datanet.csv", header = True)
df.printSchema()

from pyspark.sql.functions import expr
from pyspark.sql.functions import locate, upper

teams = ["Boston", "Celtics"]
def team_member(column, team_string):
    return locate(team_string.upper(), column).cast("boolean").alias("is_" + c)

seletedcolumn = [team_member(df.Team, c) for c in teams]
seletedcolumn.append(expr("*"))

df.select(*seletedcolumn).where(expr("is_Boston or is_Celtics")).select("Team").show(3)