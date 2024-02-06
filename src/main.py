import concurrent
import datetime
import json
import logging
import os
import threading
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

from google.cloud import bigquery
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber.scheduler import ThreadScheduler
import signal

import metrics
from opencensus.stats import aggregation
from opencensus.stats import stats

MESURE_ERRORS_NAME = "gametuner_enrich_bad_sink/error"
MESURE_NUM_OF_MESSAGES_NAME = "gametuner_enrich_bad_sink/number_of_messages"

MESURE_ERRORS =  metrics.add_metric(MESURE_ERRORS_NAME, "Number of errors in service", "1", aggregation.SumAggregation())
MEASURE_NUM_OF_MESSAGE = metrics.add_metric(MESURE_NUM_OF_MESSAGES_NAME, "Number of sank messages", "1", aggregation.SumAggregation())

project_id = os.environ.get('GCP_PROJECT_ID')
subscription_enrich_bad = os.environ.get('ENRICH_BAD_SUB')
bigquery_dataset = os.environ.get('BIGQUERY_DATASET')
bigquery_table = os.environ.get('BIGQUERY_TABLE')
bigquery_table_id = f"{project_id}.{bigquery_dataset}.{bigquery_table}"

processed_messages = 0
processed_messages_lock = threading.Lock()
sent_messages = {}

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_enrich_bad)

bigquery_client = bigquery.Client()

streaming_pull_future = None

def callback(message):
    global processed_messages
    try:
        #TODO: handle errors
        enrich_bad_data = json.loads(message.data)
        
        msg_schema = str(enrich_bad_data["schema"])
        msg_data = json.dumps(enrich_bad_data["data"])

        row_to_insert = [{"load_tstamp": "AUTO", "schema": msg_schema, "data": msg_data}]

        errors = bigquery_client.insert_rows_json(bigquery_table_id, row_to_insert)  # Make an API request.
        if errors == []:
            metrics.record_metric(MEASURE_NUM_OF_MESSAGE, 1)
            with processed_messages_lock:
                processed_messages = processed_messages + 1     
    except Exception as e:
        logging.error(e)
        metrics.record_metric(MESURE_ERRORS, 1)

    message.ack()


def exit_gracefully(sig, frame):
    logging.info("Stopping subscriber")
    global streaming_pull_future
    streaming_pull_future.cancel()


def setup():
    signal.signal(signal.SIGINT, exit_gracefully)
    signal.signal(signal.SIGTERM, exit_gracefully)

    
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')  
   
    metrics.start_exporter()


def start_subscriber():
    global streaming_pull_future
    
    streaming_pull_future = subscriber.subscribe(
        subscription_path,
        callback=callback,
        await_callbacks_on_shutdown=True,
        scheduler=ThreadScheduler(ThreadPoolExecutor(max_workers=5)),
    )
    
    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        logging.info("Starting subcribers")

        # give signal handlers a chance
        while True:
            try:
                streaming_pull_future.result(timeout=5)
                break
            except concurrent.futures._base.TimeoutError:
                logging.info(f"TimeoutError. Processed {processed_messages} messages")
                pass

        with processed_messages_lock:
            logging.info(f"Subscriber stopped. Waiting to publish all messages in queue. Number of messages in queue: {len(sent_messages)}")
            futures.wait(sent_messages, return_when=futures.ALL_COMPLETED)
            logging.info(f"Subscriber stopped. It processed {processed_messages} messages.")

if __name__ == "__main__":
    setup()
    start_subscriber()

