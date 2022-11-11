import boto3
from botocore.exceptions import ClientError
import time
import timeit
from datetime import timedelta

def start_a_job(job_name, argument_dictionary):
    """Function: start_a_job           
        Input Aruguments: job_name, argument_dictionary
        Return: response
        Description: This function uses boto3 client for glue to start a particular job."""
    session = boto3.session.Session()
    glue_client = session.client('glue', region_name='us-east-2')
    try:
        response = glue_client.start_job_run(
            JobName = job_name,
            Arguments = argument_dictionary)
        return response
    except ClientError as e:
      print("boto3 client error in job: " + e.__str__())
    except Exception as e:
      print("Unexpected error in job: " + e.__str__())


def stop_a_job(job_name, job_run_id):
    """Function: stop_a_job           
        Input Aruguments: job_name, job_run_id or could a list of ids
        Return: response
        Description: This function uses boto3 client for glue to stop a particular job."""
    client = boto3.client('glue')
    try:
        response = client.batch_stop_job_run(
            JobName=job_name,
            JobRunIds=[job_run_id]
        )
        print(response)
    except Exception as e:
      print("Unexpected error in job: " + e.__str__())


def wait_until_job_completed(job_name, job_run_id):
    """Function: wait_until_job_completed           
        Input Aruguments: job_name, job_run_id
        Return: None
        Description: This function waits until job with a specific run_id reaches SUCCEEDED or FAILED state."""

    timeout_seconds = 1000 #Can be changed as per requirements
    retry_seconds = 180 #3 mins retry seconds       
    job_start_time = time.time()
    start_time = timeit.default_timer()
    abort_time = start_time + timeout_seconds
    previous_status = None
    while True:
        client = boto3.client("glue")
        status = client.get_job_run(JobName=job_name, RunId = job_run_id).get("JobRun").get("JobRunState")
        if status != previous_status:
            print(f"Job {job_name} status: {status.lower()}.")
            previous_status = status
        if status == "SUCCEEDED":
            return 0
        if status == "FAILED":
            error_message = client.get_job_run(JobName=job_name, RunId = job_run_id).get("JobRun").get("ErrorMessage")
            print(f"Error Message: {error_message}")
            print("Job has failed. Exiting orchestration flow.")
            exit()
        if timeit.default_timer() > abort_time:
            print(f"Stopping job {job_name}. The allocated time of {timeout_seconds} seconds have elapsed.")
            response = client.batch_stop_job_run(JobName=job_name, JobRunIds=[job_run_id.get("JobRunId")])
        time.sleep(retry_seconds)
        elapsed_time = str(timedelta(seconds=round(time.time() - job_start_time)))
        print(f"Elapsed time: {elapsed_time}s\n")
