import boto3
import os
import urllib.parse
import logging

# ---------------------------------------------------
# LOGGING SETUP
# ---------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------
# AWS CLIENTS
# ---------------------------------------------------
glue = boto3.client("glue")

# ---------------------------------------------------
# LAMBDA HANDLER
# ---------------------------------------------------
def lambda_handler(event, context):
    """
    Triggered by S3 ObjectCreated events on the raw/ prefix.
    Starts a Glue job for uploaded CSV files.
    """

    # ---------------------------------------------------
    # ENVIRONMENT VARIABLES (FROM CLOUDFORMATION)
    # ---------------------------------------------------
    ENV = os.environ["ENV"]
    GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]
    DATA_BUCKET = os.environ["DATA_BUCKET"]

    # ---------------------------------------------------
    # EXTRACT S3 DETAILS
    # ---------------------------------------------------
    try:
        record = event["Records"][0]
        bucket_name = record["s3"]["bucket"]["name"]
        object_key = urllib.parse.unquote_plus(
            record["s3"]["object"]["key"]
        )
    except (KeyError, IndexError) as e:
        logger.error("Invalid S3 event structure", exc_info=True)
        raise e

    logger.info(f"Environment        : {ENV}")
    logger.info(f"Bucket             : {bucket_name}")
    logger.info(f"Object key         : {object_key}")

    # ---------------------------------------------------
    # VALIDATE BUCKET (SAFETY CHECK)
    # ---------------------------------------------------
    if bucket_name != DATA_BUCKET:
        logger.warning(
            f"Event bucket {bucket_name} does not match expected bucket {DATA_BUCKET}"
        )
        return {
            "status": "SKIPPED",
            "reason": "Unexpected bucket"
        }

    # ---------------------------------------------------
    # PROCESS ONLY RAW CSV FILES
    # ---------------------------------------------------
    if not object_key.startswith("raw/"):
        logger.info("Skipping object outside raw/ prefix")
        return {
            "status": "SKIPPED",
            "reason": "Not under raw/ prefix"
        }

    if not object_key.lower().endswith(".csv"):
        logger.info("Skipping non-CSV file")
        return {
            "status": "SKIPPED",
            "reason": "Not a CSV file"
        }

    # ---------------------------------------------------
    # BUILD INPUT / OUTPUT PATHS
    # ---------------------------------------------------
    input_path = f"s3://{bucket_name}/{object_key}"

    # Example output:
    # s3://project-dev-ci-cd-21166/clean/netflix/
    output_path = f"s3://{DATA_BUCKET}/clean/"

    logger.info(f"Input path          : {input_path}")
    logger.info(f"Output path         : {output_path}")
    logger.info(f"Glue job name       : {GLUE_JOB_NAME}")

    # ---------------------------------------------------
    # START GLUE JOB
    # ---------------------------------------------------
    try:
        response = glue.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--ENV": ENV,
                "--INPUT_PATH": input_path,
                "--OUTPUT_PATH": output_path
            }
        )
    except Exception as e:
        logger.error("Failed to start Glue job", exc_info=True)
        raise e

    job_run_id = response["JobRunId"]

    logger.info(f"Glue job started successfully")
    logger.info(f"JobRunId            : {job_run_id}")

    return {
        "status": "SUCCESS",
        "job_run_id": job_run_id,
        "environment": ENV,
        "input_path": input_path,
        "output_path": output_path
    }
