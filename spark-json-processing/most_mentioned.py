import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = (SparkSession
    .builder
    .appName("flatten")
    .master("local")
    .getOrCreate()
)

DATA_PATH = os.getenv("DATA_PATH")

df = spark.read.json(path=f"{DATA_PATH}/*.json", schema=None)

# The schema that we want:
# person, mentioned_count

most_mentioned_df = (
    df
    .select(
        F.explode(F.col("entities.persons")).alias("person_struct")
    )
    .withColumn("person", F.col("person_struct.name"))
    .drop("person_struct")
    .groupBy("person")
    .agg(F.count("*").alias("mentioned_count"))
    .orderBy(F.desc("mentioned_count"))
)

most_mentioned_df.show(10)