# As per Task 1 write the python script for task2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round as spark_round

spark = SparkSession.builder.appName("EngagementByAge").getOrCreate()

posts_df = spark.read.option("header", True).csv("input/posts.csv")
users_df = spark.read.option("header", True).csv("input/users.csv")

posts_df = posts_df.withColumn("Likes", col("Likes").cast("int")) \
                   .withColumn("Retweets", col("Retweets").cast("int"))

joined_df = posts_df.join(users_df, on="UserID", how="inner")

engagement_by_age = (
    joined_df
    .groupBy("AgeGroup")
    .agg(
        spark_round(avg("Likes"), 2).alias("AvgLikes"),
        spark_round(avg("Retweets"), 2).alias("AvgRetweets")
    )
    .orderBy(col("AvgLikes").desc())
)

engagement_by_age.show(truncate=False)

engagement_by_age.toPandas().to_csv("outputs/engagement_by_age.csv", index=False)

spark.stop()
