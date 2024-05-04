import matplotlib.pyplot as plt

most_common_actor_df = df.filter(df['cast'] == most_common_actor_name)
actor_appearances_by_year = most_common_actor_df.groupBy('release_year').count()
actor_appearances_by_year_pd = actor_appearances_by_year.toPandas()
actor_appearances_by_year_pd.plot(kind='bar', x='release_year', y='count', title='Number of Appearances by Year for the Most Common Actor')
plt.xlabel('Release Year')
plt.ylabel('Number of Appearances')
plt.show()
