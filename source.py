from confluent_kafka import Producer
import datetime
import numpy as np
import csv
from time import sleep

header = ["user_id", "movie_id", "event_type", "event_payload", "timestamp"]

#This function is to generate new interaction data.
#Each record will first be produced to kafka for streamin
#and then it will be appended to a CSV file for batch
#processing
def generateRecord() -> list:
    #We use this condition to simulate a random missing value
    if np.random.choice(np.arange(1,3),1,p=[0.95,0.05])[0] == 1:
        user_id = np.random.randint(low=1, high=2001,size=(1))[0]
    else:
        user_id = None

    if np.random.choice(np.arange(1,3),1,p=[0.95,0.05])[0] == 1:
        movie_id = np.random.randint(low=1, high=101,size=(1))[0]
    else:
        movie_id = None

    evTyp = np.random.randint(low=1, high=3,size=(1))[0]
    event_type = 'watch' if evTyp == 1 else 'rate'
    if np.random.choice(np.arange(1,3),1,p=[0.95,0.05])[0] == 1:
        ev_payload = np.random.randint(low=1, high=101, size=(1))[0] if evTyp == 1 else \
        np.random.randint(low=1, high=11, size=(1))[0]
    else:
        ev_payload = None
    timestamp = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
    return [user_id, movie_id, event_type, ev_payload, timestamp]

def kafkaProduce(broker:str, topic:str, msg:str) -> None:
# pr = Producer({'bootstrap.servers': 'localhost:9094'})
    pr = Producer({'bootstrap.servers': broker})
    # pr.produce('t1', data.encode('utf-8'))
    pr.produce(topic, msg.encode('utf-8'))
    pr.flush()

def batchFile(msg:list) -> None:
    with open("test.csv", "a+") as f:
        writer = csv.writer(f)

        writer.writerow(msg)

while True:
    #First generate new record
    data = generateRecord()
    #Then insert it into Kafka
    kafkaProduce("localhost:9094", "t1", ','.join(map(str,data)).replace('None',''))
    #Finally insert it into CSV
    batchFile(data)
    sleep(1)