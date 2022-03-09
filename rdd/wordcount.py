from pyspark import SparkConf, SparkContext
import findspark

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))


def startswiths(individual):
    return individual.startswith("S")


myc = "Spark The Definitive Guide : Big Data".split(" ")

words = sc.parallelize(myc)

words2 = words.map(lambda word: (word, word[0], word.startswith("S"))).collect()
print(words2, "\n")

myc1 = "Spark The Definitive Guide : Big Data".split(" ")
words = sc.parallelize(myc1)
words3 = words.map(lambda word: (word, word[0], word.startswith("S")))
for i in words3.collect():
    print(i)
