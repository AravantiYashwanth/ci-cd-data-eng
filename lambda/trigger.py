import boto3
import os
import urllib.parse
import logging
from typing import Dict, Any

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
def lambda_handler(event: Dict[str, Any], context):
    """
    Triggered by S3 ObjectCreated events on the raw/ prefix.
    Starts a Glue job for uploaded CSV files.
    """

    # ---------------------------------------------------
    # ENVIRONMENT VARIABLES (FROM CLOUDFORMATION)
    # ---------------------------------------------------
    ENV = os.environ.get("ENV")
    GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME")
    DATA_BUCKET = os.environ.get("DATA_BUCKET")

    if not ENV or not GLUE_JOB_NAME or not DATA_BUCKET:
        logger.error(
            "Missing environment variables",
            extra={
                "ENV": ENV,
                "GLUE_JOB_NAME": GLUE_JOB_NAME,
                "DATA_BUCKET": DATA_BUCKET
            }
        )
        raise RuntimeError("Lambda environment not configured correctly")

    logger.info(
        "Lambda configuration loaded",
        extra={
            "environment": ENV,
            "glue_job": GLUE_JOB_NAME,
            "data_bucket": DATA_BUCKET
        }
    )

    results = []

    # ---------------------------------------------------
    # PROCESS ALL RECORDS (S3 MAY SEND MULTIPLE)
    # ---------------------------------------------------
    for record in event.get("Records", []):
        try:
            bucket_name = record["s3"]["bucket"]["name"]
            object_key = urllib.parse.unquote_plus(
                record["s3"]["object"]["key"]
            )
        except KeyError:
            logger.warning("Skipping invalid S3 record structure")
            continue

        logger.info(
            "S3 event received",
            extra={
                "bucket": bucket_name,
                "object_key": object_key
            }
        )

        # ---------------------------------------------------
        # VALIDATE BUCKET
        # ---------------------------------------------------
        if bucket_name != DATA_BUCKET:
            logger.info(
                "Skipping event from unexpected bucket",
                extra={"bucket": bucket_name}
            )
            continue

        # ---------------------------------------------------
        # PROCESS ONLY raw/ CSV FILES
        # ---------------------------------------------------
        if not object_key.startswith("raw/"):
            logger.info(
                "Skipping object outside raw/ prefix",
                extra={"object_key": object_key}
            )
            continue

        if not object_key.lower().endswith(".csv"):
            logger.info(
                "Skipping non-CSV file",
                extra={"object_key": object_key}
            )
            continue

        # ---------------------------------------------------
        # BUILD INPUT / OUTPUT PATHS
        # ---------------------------------------------------
        input_path = f"s3://{bucket_name}/{object_key}"
        output_path = f"s3://{DATA_BUCKET}/clean/"

        logger.info(
            "Starting Glue job",
            extra={
                "glue_job": GLUE_JOB_NAME,
                "input_path": input_path,
                "output_path": output_path
            }
        )

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
        except Exception:
            logger.exception("Failed to start Glue job")
            continue

        job_run_id = response.get("JobRunId")

        logger.info(
            "Glue job started successfully",
            extra={
                "job_run_id": job_run_id,
                "input_path": input_path
            }
        )

        results.append(
            {
                "status": "SUCCESS",
                "job_run_id": job_run_id,
                "bucket": bucket_name,
                "object_key": object_key
            }
        )

    # ---------------------------------------------------
    # FINAL RESPONSE
    # ---------------------------------------------------
    if not results:
        return {
            "status": "NO_ACTION",
            "message": "No eligible S3 objects found"
        }

    return {
        "status": "SUCCESS",
        "environment": ENV,
        "jobs_started": results
    }
