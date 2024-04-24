from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Task1")
sc = SparkContext(conf=conf)

input_file = "hdfs://localhost:9000/user/hadoop/input/input.txt"
lines = sc.textFile(input_file)

line_count = lines.count()

print("Number of lines:", line_count)

sc.stop()
