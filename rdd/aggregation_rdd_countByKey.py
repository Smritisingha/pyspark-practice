import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

mycollection = "Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words = sc.parallelize(mycollection)
chars = words.flatMap(lambda word: word.lower())
kvchar = chars.map(lambda letter: (letter, 1))



countbykey = kvchar.countByKey()
print(countbykey)

for i in countbykey:
    print(countbykey)

# by countByKey() You can count the number of elements for each key



