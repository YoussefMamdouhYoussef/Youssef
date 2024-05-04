movies_df = df.filter(df['type'] == "Movie")
genre_counts = movies_df.groupBy('listed_in').count()
most_popular_genre = genre_counts.orderBy(desc('count')).first()
most_popular_genre_name = most_popular_genre['listed_in']
print(most_popular_genre_name)
