import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Task4")
sc = SparkContext(conf=conf)

input_file = "hdfs://localhost:9000/user/hadoop/input/input.txt"
lines = sc.textFile(input_file)

word_counts = lines.flatMap(lambda line: re.sub(r'[^a-zA-Z\s]', '', line.lower()).split())
                   .map(lambda word: (word, 1))
                   .reduceByKey(lambda x, y: x + y)
                   .map(lambda x: (x[1], x[0]))
                   .sortByKey(False)
                   .take(10)

print("Top 10 words:")
for count, word in word_counts:
    print(word, count)

sc.stop()
