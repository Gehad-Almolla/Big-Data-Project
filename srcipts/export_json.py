from pyspark.sql import SparkSession

# =========================
# START SPARK SESSION
# =========================

spark = SparkSession.builder \
    .appName("ExportJSON") \
    .getOrCreate()

# =========================
# LOAD CLEAN PARQUET FILES
# =========================

reviews_clean = spark.read.parquet(
    "/workspace/output/clean_reviews.parquet"
)

business_clean = spark.read.parquet(
    "/workspace/output/clean_business.parquet"
)

# =========================
# EXPORT CLEAN JSON FILES
# =========================

reviews_clean.write.mode("overwrite").json(
    "/workspace/output/clean_reviews_json"
)

business_clean.write.mode("overwrite").json(
    "/workspace/output/clean_business_json"
)

print("JSON EXPORT FINISHED!")