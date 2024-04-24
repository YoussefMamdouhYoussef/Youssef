import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Task3")
sc = SparkContext(conf=conf)

input_file = "hdfs://localhost:9000/user/hadoop/input/input.txt"
lines = sc.textFile(input_file)

word_count = lines.flatMap(lambda line: re.sub(r'[^a-zA-Z\s]', '', line.lower()).split())
                 .count()

print("Number of words without specials:", word_count)

sc.stop()
