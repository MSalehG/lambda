import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter, AsIs
import sys
from confluent_kafka import Producer
import datetime
import numpy as np
import csv

def createMovieDataset(names) -> list:
    dataset = []
    for id in range(1,21):
        name = names[id-1]
        #Generate a random release year between 1998 and 2010 for the movie
        year=np.random.choice(np.arange(1998,2010))
        #To create a float between 1 to 10
        score = round(np.random.normal(loc=7, scale=0.6),1)
        ln = np.random.randint(low=1, high=4,size=(1))[0]
        if ln == 1:
            language = "English"
        elif ln == 2:
            language = "Spanish"
        else:
            language = "Japenese"

        #We append tuples inside the returned dataset
        #to make it convenient when we want to insert this
        #dataset into the Postgres instance
        dataset.append((id, name, year, score, language))

    return dataset

def postgresConnector(host="127.0.0.1", port="5432", user="postgres",\
                      password="postgres", db="my_database", mode="select", query="", input=[]) -> list:
    # This command is to prevent errors
    # when inserting np.int64 data into Postgres
    register_adapter(np.int64, AsIs)

    result = []
    try:
        # Connect to an existing database
        connection = psycopg2.connect(user=user,
                                      password=password,
                                      host=host,
                                      port=port,
                                      database=db)

        # Create a cursor to perform database operations
        cursor = connection.cursor()

        if mode == "select":
            cursor.execute(query)
            result = cursor.fetchall()

        elif mode == "insert":
            execute_values(cursor, query, input)
            connection.commit()
            result.append(["success"])

        elif mode == "create":
            cursor.execute(query)
            connection.commit()
            result.append(["success"])

        connection.close()
        return result

    except:
        info = sys.exc_info()
        print(f"There was a {str(info[0])} error. Details are: {str(info[1])}")
        result.append(["failed"])
        return result

#This function is to generate new interaction data.
#Each record will first be produced to kafka for streamin
#and then it will be appended to a CSV file for batch
#processing
def generateRecord(users=1000, movies=20) -> list:
    #We use this condition to simulate a random missing value
    if np.random.choice(np.arange(1,3),1,p=[0.95,0.05])[0] == 1:
        user_id = np.random.randint(low=1, high=users+1,size=(1))[0]
    else:
        user_id = None

    if np.random.choice(np.arange(1,3),1,p=[0.95,0.05])[0] == 1:
        movie_id = np.random.randint(low=1, high=movies+1,size=(1))[0]
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

def batchFile(address, msg:list) -> None:
    with open(address, "a+") as f:
        writer = csv.writer(f)

        writer.writerows([msg])