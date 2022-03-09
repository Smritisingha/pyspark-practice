import findspark

from pyspark import SparkConf, SparkContext

findspark.init()

import random

sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

mycollection = "Spark The Definitive Guide: Big Data Processing Made Simple".split(" ")
words = sc.parallelize(mycollection)

distinctchar = words.flatMap(lambda word: list(word.lower())).distinct().collect()
samplemap = dict(map(lambda c: (c, random.random()), distinctchar))

samplebykey = words.map(lambda word: (word.lower()[0], word)).sampleByKey(True, samplemap, 6).collect()
print(samplebykey)

#Return a subset of this RDD sampled by key (via stratified sampling).
# Create a sample of this RDD using variable sampling rates for different keys as specified by fractions,
# a key to sampling rate map.