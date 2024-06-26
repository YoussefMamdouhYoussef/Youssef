top_10_words_df = word_counts_df.orderBy(desc("Count")).limit(10)
top_10_words_pd = top_10_words_df.toPandas()
top_10_words_pd.plot(kind='bar', x='Word', y='Count', title='Top 10 Most Popular Words')
plt.xlabel('Word')
plt.ylabel('Count')
plt.show()
