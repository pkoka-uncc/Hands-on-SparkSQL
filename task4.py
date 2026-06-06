from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, round as spark_round

spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

posts_df = spark.read.option("header", True).csv("input/posts.csv")
users_df = spark.read.option("header", True).csv("input/users.csv")

verified_users_df = users_df.filter(col("Verified") == "True")

joined_df = posts_df.join(verified_users_df, on="UserID", how="inner")

top_verified_users = (
    joined_df
    .groupBy("UserID", "Username", "Country", "AgeGroup")
    .agg(
        spark_sum("Likes").alias("TotalLikes"),
        spark_sum("Retweets").alias("TotalRetweets"),
        count("PostID").alias("PostCount"),
        spark_round(
            (spark_sum("Likes") + spark_sum("Retweets")), 2
        ).alias("TotalEngagement")
    )
    .orderBy(col("TotalEngagement").desc())
    .limit(10)
)

top_verified_users.show(truncate=False)

top_verified_users.toPandas().to_csv("outputs/top_verified_users.csv", index=False)

spark.stop()