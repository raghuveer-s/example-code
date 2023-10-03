import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = (SparkSession
    .builder
    .appName("flatten")
    .master("local")
    .getOrCreate()
)

df = spark.read.json(path="/home/raghuveer/data/US_financial_news/2018_01_112b52537b67659ad3609a234388c50a/*.json", schema=None)

# The schema that we want:

print(df.schema.json())