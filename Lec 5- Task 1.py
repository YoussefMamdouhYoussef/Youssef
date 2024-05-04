import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

data_path = "/content/sample_data/netflix_movies.json"
session = SparkSession.builder.master("local").appName("Dataframes").getOrCreate()
df = session.read.json(data_path, multiLine=True)
actor_counts = df.groupBy('cast').count()
most_common_actor = actor_counts.orderBy(desc('count')).first()
most_common_actor_name = most_common_actor['cast']
print(most_common_actor_name)
