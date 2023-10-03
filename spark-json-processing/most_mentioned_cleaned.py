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

# Cleaning logic for person name:
# 1. Split into first_name and last_name
# 2. Duplicate first_name into last_name_fname
# 3. In another dataframe, filter where last_name is null. This gives us a dataframe where last_name_fname is the true last name.
# 4. Join these two dataframes on last name, add the counts and use the person name from the first dataframe.

base_df = (
    most_mentioned_df
    .fillna(0)
    .withColumn("person_names", F.split(F.col("person"), " ", limit=2))
    .withColumn("first_name", F.col("person_names").getItem(0))
    .withColumn("last_name", F.col("person_names").getItem(1))
)

# Add filter in this dataframe to get quick results
# Example: Add .filter("person like '%clinton%'") at the end of the block
filtered_df1 = (
    base_df
    .withColumn("last_name_fname", F.col("last_name"))
    .where("last_name is not null")
)

# Move the first name to last name columns wherever last name is null
filtered_df2 = (
    base_df
    .select("first_name", "last_name", "mentioned_count")
    .where("last_name is null")
    .withColumnRenamed("mentioned_count", "mentioned_count_2")
    .withColumn("last_name_fname", F.col("first_name"))
    .select("last_name_fname", "mentioned_count_2")
)

windowSpec = Window.partitionBy("last_name_fname").orderBy(F.desc("total_mentions"))

# df4 = (
#     df2
#     .join(df3, how="left", on="last_name_fname")
#     .fillna(0, subset=["mentioned_count_2"])
#     .withColumn("total_mentions", F.when(F.col("mentioned_count") != F.col("mentioned_count_2"), F.col("mentioned_count") + F.col("mentioned_count_2")).otherwise(F.col("mentioned_count")))
#     .withColumn("rn", F.row_number().over(windowSpec))
#     .where("rn = 1")
#     .drop("last_name2", "mentioned_count", "mentioned_count_2", "person_names", "first_name", "last_name", "rn")
#     .orderBy("total_mentions", "person", ascending=False)
# )

final_df = (
    filtered_df1
    .join(filtered_df2, how="left", on="last_name_fname")
    .fillna(0, subset=["mentioned_count_2"])
    .withColumn("total_mentions", F.col("mentioned_count") + F.col("mentioned_count_2"))
    .withColumn("rn", F.row_number().over(windowSpec))
    .where("rn = 1")
    .orderBy("total_mentions", "person", ascending=False)
    .select("person", "total_mentions")
)

final_df.show(20)