import boto3
import os

glue = boto3.client("glue")

def lambda_handler(event, context):
    env = os.environ["ENV"]

    response = glue.start_job_run(
        JobName=os.environ["GLUE_JOB_NAME"],
        Arguments={
            "--ENV": env,
            "--INPUT_PATH": f"s3://project-{env}-raw-cicd/netflix_titles.csv",
            "--OUTPUT_PATH": f"s3://project-{env}-clean-cicd/netflix/"
        }
    )

    return {
        "status": "SUCCESS",
        "job_run_id": response["JobRunId"]
    }
