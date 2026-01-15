import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print(f"Glue job started in environment: {args['environment']}")

# Add ETL logic here
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

job.commit()
