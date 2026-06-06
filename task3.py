from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round as spark_round, when

spark = SparkSession.builder.appName("SentimentEngagement").getOrCreate()

posts_df = spark.read.option("header", True).csv("input/posts.csv")
posts_df = posts_df.withColumn("Likes", col("Likes").cast("int")).withColumn("Retweets", col("Retweets").cast("int")).withColumn("SentimentScore", col("SentimentScore").cast("double"))
posts_df = posts_df.withColumn("SentimentCategory", when(col("SentimentScore") > 0.2, "Positive").when(col("SentimentScore") < -0.2, "Negative").otherwise("Neutral"))

sentiment_engagement = posts_df.groupBy("SentimentCategory").agg(spark_round(avg("Likes"), 2).alias("AvgLikes"), spark_round(avg("Retweets"), 2).alias("AvgRetweets"), spark_round(avg("SentimentScore"), 4).alias("AvgSentimentScore")).orderBy(col("AvgSentimentScore").desc())

sentiment_engagement.show(truncate=False)

sentiment_engagement.toPandas().to_csv("outputs/sentiment_engagement.csv", index=False)

spark.stop()