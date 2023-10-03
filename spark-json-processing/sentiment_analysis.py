import math
import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from textblob import TextBlob
from typing import List

spark = (SparkSession
    .builder
    .appName("flatten")
    .master("local")
    .getOrCreate()
)

DATA_PATH = os.getenv("DATA_PATH")

df = spark.read.json(path=f"{DATA_PATH}/*.json", schema=None)

def sentiment_score_1(persons:List, locations:List, organizations:List) -> float:
    def fn(s:str):
        if s == "positive":
            return 1
        elif s == "negative":
            return -1
        else:
            return 0
    
    s1 = sum(map(fn, [p.sentiment for p in persons])) / max(len(persons), 1)
    s2 = sum(map(fn, [l.sentiment for l in locations])) / max(len(locations), 1)
    s3 = sum(map(fn, [o.sentiment for o in organizations])) / max(len(organizations), 1)

    return (s1 + s2 + s3) / 3

def sentiment_score_2(text) -> float:
    return TextBlob(text).sentiment.polarity

# Creating an user defined function
sscore1 = F.udf(sentiment_score_1, FloatType())
sscore2 = F.udf(sentiment_score_2, FloatType())

# The schema that we want:
# url, sentiment_score

flattened_df = (df
    .select(
        F.col("thread.url").alias("url"),
        F.col("entities.persons").alias("persons"),
        F.col("entities.locations").alias("locations"),
        F.col("entities.organizations").alias("organizations"),
        F.col("text"),
        F.to_timestamp(F.col("published")).alias("published_at")
    )
)

# Using udf in the transforms
final_df = (
    flattened_df
    .select(
        F.col("url"), 
        sscore1(F.col("persons"), F.col("locations"), F.col("organizations")).alias("sscore1"),
        sscore2(F.col("text")).alias("sscore2")
    )
    .withColumn("sentiment_score", (F.col("sscore1") + F.col("sscore2")) / 2)
    .drop("sscore1", "sscore2")
    .orderBy("sentiment_score", ascending=False)
)

final_df.show(10)