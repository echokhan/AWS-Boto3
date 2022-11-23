import boto3
from botocore.exceptions import ClientError
import time
import timeit

def start_a_crawler(crawler_name):
    session = boto3.session.Session()
    crawler_glue_client = session.client('glue')
    try:
       response = crawler_glue_client.start_crawler(Name=crawler_name)
       return response
    except ClientError as e:
       print("boto3 client error in start_a_crawler: " + e.__str__())
    except Exception as e:
       print("Unexpected error in start_a_crawler: " + e.__str__())

       
def stop_a_crawler(crawler_name):
    session = boto3.session.Session()
    crawler_glue_client = session.client('glue')
    try:
       response = crawler_glue_client.stop_crawler(Name=crawler_name)
       return response
    except ClientError as e:
       print("boto3 client error in stop_a_crawler: " + e.__str__())
    except Exception as e:
       print("Unexpected error in stop_a_crawler: " + e.__str__())


def wait_until_crawler_ready(crawler_name) -> None:
    retry_seconds = 5 #5s interval after which crawler status is checked again
    timeout_seconds = 600 #10 mins timeout time
    start_time = timeit.default_timer()
    abort_time = start_time + timeout_seconds
    state_previous = None
    while True:
        client = boto3.client("glue")
        response_get = client.get_crawler(Name=crawler_name)
        state = response_get["Crawler"]["State"]
        if state != state_previous:
            print(f"Crawler {crawler_name} is {state.lower()}.")
            state_previous = state
        if state == "READY":  # Other known states: RUNNING, STOPPING
            return
        if timeit.default_timer() > abort_time:
            print(f"Failed to crawl {crawler_name}. The allocated time of {timeout_seconds} seconds have elapsed.")
        time.sleep(retry_seconds)
        
        
def list_of_crawlers():
   session = boto3.session.Session()
   glue_client = session.client('glue')
   try:
      list =  glue_client.list_crawlers()
      return list['CrawlerNames']
   except ClientError as e:
      print("boto3 client error in list_of_crawlers: " + e.__str__())
   except Exception as e:
      print("Unexpected error in list_of_crawlers: " + e.__str__())