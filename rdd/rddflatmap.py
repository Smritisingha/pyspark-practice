import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

data = ["Project Gutenberg’s",
        "Alice’s Adventures in Wonderland",
        "Project Gutenberg’s",
        "Adventures in Wonderland",
        "Project Gutenberg’s"]
rdd = sc.parallelize(data)
for element in rdd.collect():
    print(element)


rdd2 = rdd.flatMap(lambda x: x.split(" "))
print(rdd2.collect())
for element in rdd2.collect():
    print(element)



