import boto3
import os
import urllib.parse
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue = boto3.client("glue")

def lambda_handler(event, context):
    """
    Triggered by S3 ObjectCreated event.
    Starts Glue job for the uploaded CSV file.
    """

    ENV = os.environ["ENV"]
    GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]

    # ---------------------------------------------------
    # Extract S3 details
    # ---------------------------------------------------
    record = event["Records"][0]
    bucket_name = record["s3"]["bucket"]["name"]
    object_key = urllib.parse.unquote_plus(
        record["s3"]["object"]["key"]
    )

    logger.info(f"Received file: s3://{bucket_name}/{object_key}")
    logger.info(f"Environment : {ENV}")

    # ---------------------------------------------------
    # Process only CSV files
    # ---------------------------------------------------
    if not object_key.lower().endswith(".csv"):
        logger.info("Skipping non-CSV file")
        return {
            "status": "SKIPPED",
            "reason": "Not a CSV file"
        }

    input_path = f"s3://{bucket_name}/{object_key}"
    output_path = f"s3://project-{ENV}-clean-cicd/netflix/"

    logger.info(f"Input Path  : {input_path}")
    logger.info(f"Output Path : {output_path}")
    logger.info(f"Starting Glue job: {GLUE_JOB_NAME}")

    # ---------------------------------------------------
    # Start Glue Job
    # ---------------------------------------------------
    response = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--ENV": ENV,
            "--INPUT_PATH": input_path,
            "--OUTPUT_PATH": output_path
        }
    )

    job_run_id = response["JobRunId"]
    logger.info(f"Glue job started successfully: {job_run_id}")

    return {
        "status": "SUCCESS",
        "job_run_id": job_run_id,
        "environment": ENV,
        "input": input_path
    }
