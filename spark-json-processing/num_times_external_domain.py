import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = (SparkSession
    .builder
    .config("spark.driver.memory", "8g")
    .appName("flatten")
    .master("local")
    .getOrCreate()
)

DATA_PATH = os.getenv("DATA_PATH")

df = spark.read.json(path=f"{DATA_PATH}/*.json", schema=None)

# The schema that we want:
# external_website, news_website, num_links

flattened_df = (df
    .select(
        F.col("thread.url").alias("url"),
        F.col("thread.site").alias("domain"),
        F.col("thread.performance_score").cast("int").alias("performance_score"),
        F.col("author"),
        F.col("title"),
        F.explode("external_links").alias("external_link"),
        F.col("text"),
        F.to_timestamp(F.col("published")).alias("published_at")
    )
)

# Count number of times an external website points to the news website

links_df = (
    flattened_df
    .select(
        F.expr("parse_url(external_link, 'HOST')").alias("external_website"),
        F.col("domain").alias("news_website")
    )
    .groupBy("external_website", "news_website")
    .agg(F.count("*").alias("num_links"))
    .orderBy(F.desc("num_links"))
)

links_df.show(10)