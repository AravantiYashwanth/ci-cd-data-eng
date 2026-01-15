import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, trim, to_date

# Read arguments from Glue Job
args = getResolvedOptions(
    sys.argv,
    ["ENV", "INPUT_PATH", "OUTPUT_PATH"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

print(f"Running Glue job for ENV={args['ENV']}")

df = spark.read.option("header", "true").csv(args["INPUT_PATH"])

df_clean = (
    df
    .filter(col("title").isNotNull())
    .filter(col("type").isin("Movie", "TV Show"))
    .withColumn("country", trim(col("country")))
    .withColumn("date_added", to_date(col("date_added"), "MMMM d, yyyy"))
    .dropDuplicates(["show_id"])
)

df_clean.write.mode("overwrite").parquet(args["OUTPUT_PATH"])

print(f"{args['ENV']} Glue job completed successfully")
