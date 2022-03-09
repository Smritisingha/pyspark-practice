from pyspark.sql import SparkSession

spark: SparkSession = SparkSession.builder.master("local[1]").appName("Spark").getOrCreate()

csv = spark.read.csv("C:/Users/smrit/Downloads/orders.txt")
acc = spark.sparkContext.accumulator(0)

print(acc)


def accfunc(order_row):
    status = order_row["order_status"]
    no = order_row["order_id"]
    if status == "CLOSED":
        acc.add(order_row["count"])
    if no == 4:
        acc.add(order_row["count"])


csv.foreach(lambda order_row: accfunc(order_row))

for i in acc.value:
    print(i)

