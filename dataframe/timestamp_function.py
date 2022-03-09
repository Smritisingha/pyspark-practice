from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("df").getOrCreate()
from pyspark.sql.functions import current_date, current_timestamp
datedf = spark.range(10).withColumn("today", current_date()).withColumn("now", current_timestamp())
datedf.createOrReplaceTempView("datetable")
datedf.printSchema()
datedf.show()
spark.sql("select * from datetable").show()
from pyspark.sql.functions import date_add, date_sub, col

# lets add and subtract five days from today
datedf.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show()



#difference between no of days we use datediff function
#months_between gives number of months between two dates

from pyspark.sql.functions import datediff, months_between, to_date, lit

datedf.withColumn("weekago", date_sub(col("today"), 7)).select(datediff(col("weekago"), col("today"))).show()
datedf.select(
    to_date(lit("2016-01-01")).alias("start"),
    to_date(lit("2017-05-22")).alias("end"))\
    .select(months_between(col("start"),col("end"))).show()

#to_date allows you to convert a string to a date
spark.range(5).withColumn("date", lit("2017-01-01")).select(to_date(col("date"))).show()

dateformat = "yyyy-dd-mm"

cleandatadf = spark.range(1).select(
    to_date(lit("2017-12-11"), dateformat).alias("date"),
    to_date(lit("2017-20-12"), dateformat).alias("date2"))
cleandatadf.createOrReplaceTempView("datetable2")
spark.sql("select * from datetable2").show()

from pyspark.sql.functions import to_timestamp
cleandatadf.select(to_timestamp(col("date"), dateformat), to_timestamp(col("date2"), dateformat)).show()
