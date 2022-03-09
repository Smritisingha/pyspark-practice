from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("app").getOrCreate()
df = spark.read.option("header", "true").csv("C:/Users/smrit/Downloads/orders.txt")

df.rdd.getNumPartitions() #5

dfcol = df.coalesce(4)
dfcol.rdd.getNumPartitions()

print(dfcol)
df_coll = df.repartition(5) # if we take df.repartition(3) then it'll increase or decrease the partition by 3
df_coll.rdd.getNumPartitions() # get the number of partition
df1 = df_coll.coalesce(4) # it can decrease the number of partition but cant increase it
df1.rdd.getNumPartitions()
df.repartition(3,"order_cid").rdd.getNumPartitions()
from pyspark.sql.functions import spark_partition_id
df.repartition(3,"order_status").withColumn("partitionid", spark_partition_id()).groupBy("partitionid").count().show()

df.write.partitionBy("order_status")# inside we cannot give any partition number only give column name
# repartition will make shuffeling in the data and create only one partition inside the folder
df.repartition("order_status").write.format('csv').option("header","true").mode("overwrite").save("C:/Users/smrit/Downloads/data.txt")

# partitionby not make shuffeling in the data and partitionBY create  folder for each order_status column
df.write.format('csv').partitionBy("order_status").option("header","true").mode("overwrite").save("C:/Users/smrit/Downloads/part.txt")
part = spark.read.csv("C:/Users/smrit/Downloads/part.txt",header=True)
part.show()
