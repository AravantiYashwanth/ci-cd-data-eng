import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import col, trim, to_date
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "ENV", "INPUT_PATH", "OUTPUT_PATH"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print(f"Starting Glue job for environment: {args['ENV']}")

df = spark.read.option("header", "true").csv(args["INPUT_PATH"])

df_clean = (
    df.filter(col("title").isNotNull())
      .filter(col("type").isin("Movie", "TV Show"))
      .withColumn("country", trim(col("country")))
      .withColumn("date_added", to_date(col("date_added"), "MMMM d, yyyy"))
      .dropDuplicates(["show_id"])
)

df_clean.write.mode("overwrite").parquet(args["OUTPUT_PATH"])

print("Glue job completed successfully")

job.commit()
