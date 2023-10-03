import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = (SparkSession
    .builder
    .appName("flatten")
    .master("local")
    .getOrCreate()
)

DATA_PATH = os.getenv("DATA_PATH")

df = spark.read.json(path=f"{DATA_PATH}/*.json", schema=None)

# Get the total shares for an article

social_shares_df = (df
    .select(
        F.col("thread.url").alias("url"),
        F.col("thread.site").alias("domain"),
        F.col("thread.social.gplus.shares").alias("gplus_shares"),
        F.col("thread.social.pinterest.shares").alias("pinterest_shares"),
        F.col("thread.social.vk.shares").alias("vk_shares"),
        F.col("thread.social.linkedin.shares").alias("linkedin_shares"),
        F.col("thread.social.facebook.shares").alias("facebook_shares"),
        F.col("thread.social.stumbledupon.shares").alias("stumbleupon_shares"),
    )
    .withColumn("total_social_shares", F.col("gplus_shares") + F.col("pinterest_shares") + F.col("vk_shares") + F.col("linkedin_shares") + F.col("facebook_shares") + F.col("stumbleupon_shares"))
    .drop("gplus_shares", "pinterest_shares", "vk_shares", "linkedin_shares", "facebook_shares", "stumbleupon_shares")
    .groupBy("url", "domain")
    .agg({
        "total_social_shares" : "count"
    })
    .withColumnRenamed("count(total_social_shares)", "total_shares")
)

# Get the maximum and minimum shared articles for a news website

most_popular_df = (
    social_shares_df
    .groupBy("domain")
    .agg(
        F.first("url").alias("most_popular_article"),
        F.max("total_shares").alias("most_popular_article_num_shares")
    )
)
most_popular_df.createOrReplaceGlobalTempView("most_popular_df")

least_popular_df = (
    social_shares_df
    .groupBy("domain")
    .agg(
        F.first("url").alias("least_popular_article"),
        F.min("total_shares").alias("least_popular_article_num_shares")
    )
)
least_popular_df.createOrReplaceGlobalTempView("least_popular_df")

final_df = spark.sql(
    """
    select * 
    from global_temp.most_popular_df t1
    left join global_temp.least_popular_df t2
    on t1.domain = t2.domain
    """
)

final_df.show(10, truncate=30)