from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

import findspark
findspark.init()
sc = spark.sparkContext
df = spark.read.option("inferSchema", "true").json("C:/Users/smrit/Downloads/input.json")

df.show()
counter = sc.accumulator(0)
from pyspark.accumulators import AccumulatorParam
class StringAccumulator(AccumulatorParam):
    def zero(self, initialValue=" "):
        return " "
    def addInPlace(self,s1,s2):
        return s1.strip() + " " +s2.strip()

countervalue = sc.accumulator(" ",StringAccumulator())
-
def validate(row):
    age=row.Age
    if age < 15:
        counter.add(1)
        countervalue.add(str(age))

df.foreach(lambda x: validate(x))
