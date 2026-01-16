import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    to_timestamp,
    count,
    date_format
)
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# -------------------------------------------------------
# Resolve arguments passed from Lambda
# -------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "ENV", "INPUT_PATH", "OUTPUT_PATH"]
)

ENV = args["ENV"]
INPUT_PATH = args["INPUT_PATH"]
OUTPUT_PATH = args["OUTPUT_PATH"]

# -------------------------------------------------------
# Glue / Spark Context
# -------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print(f"Starting Glue Log Processor")
print(f"Environment  : {ENV}")
print(f"Input Path   : {INPUT_PATH}")
print(f"Output Path  : {OUTPUT_PATH}")

# -------------------------------------------------------
# 1. Read CSV Logs from S3
# -------------------------------------------------------
logs_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(INPUT_PATH)
)

# -------------------------------------------------------
# 2. Timestamp Parsing & Cleaning
# -------------------------------------------------------
logs_df = (
    logs_df
    .withColumn(
        "timestamp",
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )
    .filter(col("timestamp").isNotNull())
)

# -------------------------------------------------------
# 3. Error Count by Service
# -------------------------------------------------------
error_counts_df = (
    logs_df
    .filter(col("level") == "ERROR")
    .groupBy("service")
    .agg(count("*").alias("error_count"))
)

# -------------------------------------------------------
# 4. Status Code Aggregation
# -------------------------------------------------------
status_code_df = (
    logs_df
    .groupBy("status_code")
    .agg(count("*").alias("count"))
)

# -------------------------------------------------------
# 5. Daily Log Volume
# -------------------------------------------------------
daily_logs_df = (
    logs_df
    .withColumn("log_date", date_format(col("timestamp"), "yyyy-MM-dd"))
    .groupBy("log_date")
    .agg(count("*").alias("total_logs"))
)

# -------------------------------------------------------
# 6. Write Outputs as Parquet
# -------------------------------------------------------
error_counts_df.write.mode("overwrite").parquet(
    f"{OUTPUT_PATH}/errors_by_service"
)

status_code_df.write.mode("overwrite").parquet(
    f"{OUTPUT_PATH}/status_code_counts"
)

daily_logs_df.write.mode("overwrite").parquet(
    f"{OUTPUT_PATH}/daily_logs"
)

print("Glue Log Processing completed successfully")

job.commit()