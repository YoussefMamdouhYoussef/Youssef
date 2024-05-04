from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("Word", StringType(), True),
    StructField("Count", IntegerType(), True)
])

word_counts_df = session.createDataFrame(word_counts, schema)
word_counts_df.show()
