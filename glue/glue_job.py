import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    to_timestamp,
    count,
    date_format,
    lit
)
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# -------------------------------------------------------
# RESOLVE ARGUMENTS (FROM LAMBDA / CLOUDFORMATION)
# -------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "ENV", "INPUT_PATH", "OUTPUT_PATH"]
)

JOB_NAME = args["JOB_NAME"]
ENV = args["ENV"]
INPUT_PATH = args["INPUT_PATH"]
OUTPUT_PATH = args["OUTPUT_PATH"]

# -------------------------------------------------------
# GLUE / SPARK CONTEXT
# -------------------------------------------------------
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(JOB_NAME, args)

print("--------------------------------------------------")
print("Starting Glue Log Aggregation Job")
print(f"Job Name    : {JOB_NAME}")
print(f"Environment : {ENV}")
print(f"Input Path  : {INPUT_PATH}")
print(f"Output Path : {OUTPUT_PATH}")
print("--------------------------------------------------")

# -------------------------------------------------------
# 1. READ CSV LOGS FROM S3 (RAW PREFIX)
# -------------------------------------------------------
logs_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(INPUT_PATH)
)

if logs_df.rdd.isEmpty():
    print("No records found in input path. Exiting job gracefully.")
    job.commit()
    sys.exit(0)

# -------------------------------------------------------
# 2. TIMESTAMP PARSING & BASIC CLEANING
# -------------------------------------------------------
logs_df = (
    logs_df
    .withColumn(
        "timestamp",
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )
    .filter(col("timestamp").isNotNull())
)

# Add environment column for downstream analytics
logs_df = logs_df.withColumn("environment", lit(ENV))

# -------------------------------------------------------
# 3. ERROR COUNT BY SERVICE
# -------------------------------------------------------
errors_by_service_df = (
    logs_df
    .filter(col("level") == "ERROR")
    .groupBy("service", "environment")
    .agg(count("*").alias("error_count"))
)

# -------------------------------------------------------
# 4. STATUS CODE AGGREGATION
# -------------------------------------------------------
status_code_counts_df = (
    logs_df
    .groupBy("status_code", "environment")
    .agg(count("*").alias("count"))
)

# -------------------------------------------------------
# 5. DAILY LOG VOLUME
# -------------------------------------------------------
daily_logs_df = (
    logs_df
    .withColumn("log_date", date_format(col("timestamp"), "yyyy-MM-dd"))
    .groupBy("log_date", "environment")
    .agg(count("*").alias("total_logs"))
)

# -------------------------------------------------------
# 6. WRITE OUTPUTS AS PARQUET (CLEAN PREFIX)
# -------------------------------------------------------
(
    errors_by_service_df
    .write
    .mode("overwrite")
    .parquet(f"{OUTPUT_PATH}/errors_by_service")
)

(
    status_code_counts_df
    .write
    .mode("overwrite")
    .parquet(f"{OUTPUT_PATH}/status_code_counts")
)

(
    daily_logs_df
    .write
    .mode("overwrite")
    .parquet(f"{OUTPUT_PATH}/daily_logs")
)

print("--------------------------------------------------")
print("Glue Log Aggregation completed successfully")
print("--------------------------------------------------")

job.commit()
