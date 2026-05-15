from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

# =========================
# START SPARK SESSION
# =========================

spark = SparkSession.builder \
    .appName("YelpPreprocessing") \
    .getOrCreate()

# =========================
# LOAD AND SAMPLE DATA
# =========================

reviews_df = spark.read.json(
    "/workspace/yelp_dataset/yelp_academic_dataset_review.json"
).sample(False, 0.4, seed=42)

business_df = spark.read.json(
    "/workspace/yelp_dataset/yelp_academic_dataset_business.json"
)

# =========================
# SHOW SAMPLE DATA
# =========================

print("REVIEWS DATA")
reviews_df.show(5)

print("BUSINESS DATA")
business_df.show(5)

# =========================
# PRINT SCHEMAS
# =========================

print("REVIEWS SCHEMA")
reviews_df.printSchema()

print("BUSINESS SCHEMA")
business_df.printSchema()

# =========================
# REMOVE NULLS
# =========================

reviews_clean = reviews_df.dropna(subset=[
    'business_id',
    'user_id',
    'stars',
    'text'
])

business_clean = business_df.dropna(subset=[
    'business_id',
    'name',
    'categories'
])

# =========================
# FORMAT DATES
# =========================

reviews_clean = reviews_clean.withColumn(
    'date',
    to_timestamp(col('date'))
)

# =========================
# REMOVE INVALID RECORDS
# =========================

reviews_clean = reviews_clean.filter(
    (col('stars') >= 1) & (col('stars') <= 5)
)

reviews_clean = reviews_clean.filter(
    length(col('text')) > 5
)

# =========================
# BASIC TEXT CLEANING
# =========================

reviews_clean = reviews_clean.withColumn(
    'clean_text',
    lower(col('text'))
)

reviews_clean = reviews_clean.withColumn(
    'clean_text',
    regexp_replace(
        col('clean_text'),
        '[^a-zA-Z0-9\\s]',
        ''
    )
)

# =========================
# KEEP IMPORTANT COLUMNS
# =========================

reviews_clean = reviews_clean.select(
    'review_id',
    'business_id',
    'user_id',
    'stars',
    'date',
    'clean_text'
)

business_clean = business_clean.select(
    'business_id',
    'name',
    'city',
    'state',
    'stars',
    'review_count',
    'categories'
)

# =========================
# SHOW CLEANED DATA
# =========================

print("CLEANED REVIEWS")
reviews_clean.show(5)

print("CLEANED BUSINESS")
business_clean.show(5)

# =========================
# CREATE OUTPUT FOLDER
# =========================

os.makedirs('/workspace/output', exist_ok=True)

# =========================
# SAVE PARQUET FILES
# =========================

reviews_clean.write.mode("overwrite").parquet(
    "/workspace/output/clean_reviews.parquet"
)

business_clean.write.mode("overwrite").parquet(
    "/workspace/output/clean_business.parquet"
)


print("PREPROCESSING FINISHED!")