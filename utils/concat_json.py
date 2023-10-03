import json
import sys
from pyspark.sql import SparkSession
from pyspark import Row

spark = (SparkSession
    .builder
    .appName("flatten")
    .master("local")
    .getOrCreate()
)

df = spark.read.json(path="/home/raghuveer/data/news_tweets/news.json", schema=None)

output_file = "/home/raghuveer/data/news_tweets/news_concat.json"
    
def output_multiline(row:Row):
    f = open(output_file, 'a+')
    json_str = json.dumps(row.asDict())
    f.write(json_str)
    f.close()

df.foreach(output_multiline)