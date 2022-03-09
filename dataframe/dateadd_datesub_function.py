from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
from pyspark.sql.functions import date_add, date_sub
# lets add and subtract five days from today
