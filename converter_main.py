import glob                             # for searching for json file 
import os                               # for setting and reading environment variables
from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import time                             # for sleep function
import json;                            # to deal with json objects
import random                           # to generate random values
import csv 
import argparse
import logging

# Search the current directory for the JSON file (including the Google Pub/Sub credential) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];

#project, topic, and subscription id from the cloud
project_id = os.environ["GCP_PROJECT"];
topic_name = os.environ["TOPIC_NAME"];
subscription_id = os.environ["SUB_ID"];

# create a publisher and get the topic path for the publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

# create a subscriber to the subscriber for the project using the subscription_id
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
sub_filter = "attributes.function=\"filteredData\""  # the condition used for filtering the messages to be recieved 

#function to convert pressure and temperature measurements
def convertMeasurements(record: dict):
    record["pressure"] = float(record.get("pressure", 0.0)) / 6.895
    record["temperature"] = float(record.get("temperature", 0.0)) * 1.8 + 32
    return record

# function to handle a message being received 
def callback(message: pubsub_v1.subscriber.message.Message)-> None:
    message_data = json.loads(message.data.decode('utf-8'));

    print("Consumed record: {}".format(message_data))

    #convert message data before sending
    value = convertMeasurements(message_data)
    future = publisher.publish(topic_path, json.dumps(value).encode('utf-8'),function="convertedData");

    message.ack()

with subscriber:
    # Create a subscription with the given ID and filter for the first time, if already not existed
    try:
        subscription = subscriber.create_subscription(
            request={"name": subscription_path, "topic": topic_path, "filter": sub_filter}
        )
    except:
        pass;
    
    # Now, the subscription is already existing or has been created. 
    # The call back function will be called for each message match the filter from the topic.
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
