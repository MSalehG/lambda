from utils import generateRecord, kafkaProduce, batchFile, postgresConnector, createMovieDataset
import numpy as np
from time import sleep
import csv
from json import loads
import sys

argv = sys.argv
csvAddress = "/source/source.csv"
header = ["user_id", "movie_id", "event_type", "event_payload", "timestamp"]

#*****
#In this script first the database tables are created
#and then random data is generated real time and produced to the kafka topic
#*****

#A list of 20 movie names
movie_names = ['The Shawshank Redemption', 'The Godfather',\
               'The Dark Knight', 'The Godfather Part II', "12 Angry Men", "Schindler's List",\
               'The Lord of the Rings: The Return of the King', 'Pulp Fiction',\
               'The Lord of the Rings: The Fellowship of the Ring', 'The Good, the Bad and the Ugly',\
               'Forrest Gump', 'Fight Club', 'The Lord of the Rings: The Two Towers', 'Inception',\
               'Star Wars: Episode V - The Empire Strikes Back', 'The Matrix',\
               'Goodfellas', "One Flew Over the Cuckoo's Nest", \
               'Spider-Man: Across the Spider-Verse', 'Se7en']

#Queries to create sink tables of the architecture

metadataQuery = """CREATE TABLE IF NOT EXISTS movies (
                    id INT,
                    name VARCHAR(64),
                    year INT,
                    score FLOAT(1),
                    language VARCHAR(32))"""
allJoinedQuery_batch  = """CREATE TABLE IF NOT EXISTS allJoined_batch (
                    user_id INT,
                    movie_name  VARCHAR(64),
                    movie_year INT,
                    movie_score FLOAT(1),
                    event_type VARCHAR(6),
                    event_payload INT,
                    timestamp TIMESTAMP)"""
userGroupedQuery_batch  = """CREATE TABLE IF NOT EXISTS userGrouped_batch (
                    user_id INT,
                    event_type VARCHAR(6),
                    avg_payload INT)"""
movieGroupedQuery_batch  = """CREATE TABLE IF NOT EXISTS movieGrouped_batch (
                    name  VARCHAR(64),
                    event_type VARCHAR(6),
                    avg_payload INT,
                    score FLOAT(1),
                    year INT)"""
allJoinedQuery_stream  = """CREATE TABLE IF NOT EXISTS allJoined_stream (
                    user_id INT,
                    movie_name  VARCHAR(64),
                    movie_year INT,
                    movie_score FLOAT(1),
                    event_type VARCHAR(6),
                    event_payload INT,
                    timestamp TIMESTAMP)"""
userGroupedQuery_stream  = """CREATE TABLE IF NOT EXISTS userGrouped_stream (
                    user_id INT,
                    event_type VARCHAR(6),
                    avg_payload INT,
                    timestamp TIMESTAMP)"""
movieGroupedQuery_stream  = """CREATE TABLE IF NOT EXISTS movieGrouped_stream (
                    name  VARCHAR(64),
                    event_type VARCHAR(6),
                    avg_payload INT,
                    score FLOAT(1),
                    year INT,
                    timestamp TIMESTAMP)"""


queryList = [metadataQuery,\
             allJoinedQuery_batch, userGroupedQuery_batch, movieGroupedQuery_batch,\
             allJoinedQuery_stream, userGroupedQuery_stream, movieGroupedQuery_stream]
tableList = ["movies",
             "allJoined_batch", "userGrouped_batch", "movieGrouped_batch",\
             "allJoined_stream", "userGrouped_stream", "movieGrouped_stream"]


query = f"drop table movies"
postgresConnector(mode="create", query=query)

for query in queryList:
    postgresConnector(mode="create", query=query)

dataset = createMovieDataset(movie_names)
insertQuery = "INSERT INTO movies (id, name, year, score, language) VALUES %s"
postgresConnector(mode="insert", query=insertQuery, input=dataset)

#Uncomment this part only if you wnat to create another
#CSV file for the batch processing
# with open(csvAddress, "w") as f:
#         writer = csv.writer(f)
#         writer.writerow(header)
#
# for i in range(1000):
#     data = generateRecord()
#     batchFile(csvAddress, data)
#     rnd = np.random.choice([1/i for i in range(1,5)],1)[0]
#     sleep(rnd)

while True:
    #First generate new record
    data = generateRecord()
    #Then insert it into Kafka
    kafkaProduce("localhost:9094", argv[1], ','.join(map(str,data)).replace('None',''))

    rnd = np.random.choice([1/i for i in range(1,5)],1)[0]
    sleep(rnd)

