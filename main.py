import glob                             # for searching for json file 
import os                               # for setting and reading environment variables
from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import time                             # for sleep function
import json;                            # to deal with json objects
import random                           # to generate random values
import csv 

# Search the current directory for the JSON file (including the Google Pub/Sub credential) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
if len(files)>0:
   os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];

# set the project and topic ids
project_id="psyched-bonfire-485220-e6";
topic_name = "MS4_Design";

#set up the publisher with the topic path
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

# use the csv.DictReader class to map the 
# entries in the csv file to dictionary entries
with open('Labels.csv', mode='r') as f:
    myFile = csv.DictReader(f)
    for entries in myFile:

        id = None if entries["ID"] == "" else int(entries["ID"])
        timeData = None if entries["time"] == "" else int(float(entries["time"]))
        profileName = None if entries["profile_name"] == "" else str(entries["profile_name"])
        temperature = None if entries["temperature"] == "" else float(entries["temperature"])
        humidity = None if entries["humidity"] == "" else float(entries["humidity"])
        pressure = None if entries["pressure"] == "" else float(entries["pressure"])

        # Structure dictionary message
        msg = {"ID" : id, "time" : timeData, "profile_name" : profileName, "temperature" : temperature, "humidity" : humidity, "pressure" : pressure}

        #serialize message before sending
        record_value=json.dumps(msg).encode('utf-8');
        #try sending the message
        try:
            future = publisher.publish(topic_path, record_value, function="newData");
            future.result() # get the result back
            print("The messages {} has been published successfully".format(msg))
        except:
            print("Message failed to publish")
        
        time.sleep(.5)