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
    Starts Glue job for the uploaded file.
    """

    env = os.environ["ENV"]
    glue_job_name = os.environ["GLUE_JOB_NAME"]

    # Extract S3 details
    record = event["Records"][0]
    bucket_name = record["s3"]["bucket"]["name"]
    object_key = urllib.parse.unquote_plus(
        record["s3"]["object"]["key"]
    )

    logger.info(f"Received file: s3://{bucket_name}/{object_key}")

    # Process only CSV files
    if not object_key.lower().endswith(".csv"):
        logger.info("Skipping non-CSV file")
        return {
            "status": "SKIPPED",
            "reason": "Not a CSV file"
        }

    input_path = f"s3://{bucket_name}/{object_key}"
    output_path = f"s3://project-{env}-clean-cicd/netflix/"

    logger.info(f"Starting Glue job: {glue_job_name}")

    response = glue.start_job_run(
        JobName=glue_job_name,
        Arguments={
            "--ENV": env,
            "--INPUT_PATH": input_path,
            "--OUTPUT_PATH": output_path
        }
    )

    logger.info(f"Glue job started: {response['JobRunId']}")

    return {
        "status": "SUCCESS",
        "job_run_id": response["JobRunId"],
        "input": input_path
    }
