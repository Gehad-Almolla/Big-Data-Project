from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("YelpSentimentProject") \
    .getOrCreate()

business_df = spark.read.json(
    "hdfs://namenode:9000/project/yelp/raw/yelp_academic_dataset_business.json"
)

review_df = spark.read.json(
    "hdfs://namenode:9000/project/yelp/raw/yelp_academic_dataset_review.json"
)

print("Business rows:", business_df.count())
print("Review rows:", review_df.count())

business_df.printSchema()
review_df.select("stars", "text").show(5, truncate=False)