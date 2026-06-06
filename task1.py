from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, trim

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv")

# TODO: Split the Hashtags column into individual hashtags and count the frequency of each hashtag and sort descending
hashtag_counts = (
    posts_df
    .select(explode(split(col("Hashtags"), ",")).alias("Hashtag"))
    .select(trim(col("Hashtag")).alias("Hashtag"))
    .groupBy("Hashtag")
    .count()
    .withColumnRenamed("count", "Frequency")
    .orderBy(col("Frequency").desc())
)

hashtag_counts.show(truncate=False)

# Save result
hashtag_counts.toPandas().to_csv("outputs/hashtag_trends.csv", index=False)
