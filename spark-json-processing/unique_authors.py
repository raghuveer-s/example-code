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

# The schema that we want:
# url, domain, performance_score, author, title, social_shares, text, published_at 

flattened_df = (df
    .select(
        F.col("thread.url").alias("url"),
        F.col("thread.site").alias("domain"),
        F.col("author"),
        F.col("title"),
        F.col("thread.social").alias("socials"),
        F.col("text"),
        F.to_timestamp(F.col("published")).alias("published_at")
    )
)

# Unique authors
c = flattened_df.select("author").distinct().count()

print(c)