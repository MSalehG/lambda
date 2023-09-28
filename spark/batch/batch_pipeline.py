import pyspark.sql.types
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import pyspark.sql.functions as F
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType, FloatType
from json import loads

def readFromPostgres(sp:pyspark.sql.SparkSession, query,\
                     user, password,\
                     ptColumn, lowerBound, upperBound, numPartitions) -> pyspark.sql.DataFrame:
    dataframe = sp.read.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/my_database") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", query) \
        .option("partitionColumn", ptColumn)\
        .option("lowerBound", lowerBound)\
        .option("upperBound", upperBound)\
        .option("numPartitions", numPartitions)\
        .option("user", user) \
        .option("password", password) \
        .load()
    return dataframe

def writeToPostgres(dataframe:pyspark.sql.DataFrame, url, dbtable, user, password) -> None:
    dataframe.write.mode("append") \
        .format("jdbc") \
        .option("url", url) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", dbtable) \
        .option("user", user) \
        .option("password", password) \
        .save()

configs = loads(open("/job/configs.json", 'r').read().strip())["batch"]
conf = SparkConf().setAppName("lambda_batch")\
    .setAll(configs["sparkConf"])
spark = SparkSession.Builder()\
    .config(conf=conf).getOrCreate()

sc = spark.sparkContext
log4jLogger = sc._jvm.org.apache.log4j
log4jLogger.LogManager.getRootLogger().setLevel(log4jLogger.Level.WARN)

#Create a schema for the dataframe after reading from the csv file
schema = StructType([StructField("user_id", IntegerType(), nullable=True),\
                     StructField("movie_id", IntegerType(), nullable=True),\
                     StructField("event_type", StringType(), nullable=False),\
                     StructField("event_payload", FloatType(), nullable=True),\
                     StructField("timestamp", TimestampType(), nullable=False)])
#
# Reading from the csv with header present and ',' as the delimiter
df = spark.read.option("header", "true") \
    .option("delimiter", ",").schema(schema) \
    .csv(path=configs["csvPath"]).cache()

#Drop duplicated records
df.dropDuplicates(["user_id", "movie_id", "event_type", "event_payload"])

#In order to clean the data for a movie recommendation system
#records with user_id or movie_id as null values don't hold value
#But before we discard them we calculate the average event_payload
#over all the records to replace the null values present in the event_payload
#column. After that we discard the records with null values in either
#the user_id or the movie_id columns
avgDF = df.groupBy("event_type")\
    .avg("event_payload").collect()
avgs = {}
avgs[avgDF[0][0]] = round(avgDF[0][1],1)
avgs[avgDF[1][0]] = round(avgDF[1][1],1)
print(avgs)

#This part was more conveniently done using spark.sql
#So we first created a temp view and wrote the logic in SQL
df.createOrReplaceTempView("df")
df = spark.sql(f"""SELECT user_id, movie_id, event_type,
CASE WHEN event_type = 'watch' THEN CASE WHEN event_payload IS NULL THEN {avgs["watch"]} ELSE event_payload END
     WHEN event_type = 'rate' THEN CASE WHEN event_payload IS NULL THEN {avgs["rate"]} ELSE event_payload END END AS event_payload,
timestamp
FROM df""")

#Henceforth the behaviorData df will be used and there's no need for
#the df so we remove it from cache
behaviorData = df.na.drop(subset=["user_id", "movie_id"]).cache()
df.unpersist()


#A query to fetch movie metadata in parallel in contain it in multiple partitions in spark
pgQuery = f"""(SELECT mod({configs['partitionColumn']}, 4) as partitionKey, * from movies) as subQuery"""
pgtable = readFromPostgres(spark, user=configs["user"], password=configs["password"],\
                           query=pgQuery, ptColumn="partitionKey",\
                           lowerBound=0, upperBound=f"{int(configs['numPartitions'])}", numPartitions=configs["numPartitions"]).cache()


#This dataframe is the joined version of the untransformed data
allJoinedoinedDF = behaviorData.join(F.broadcast(pgtable), pgtable.id == behaviorData.movie_id)\
    .select(behaviorData['user_id'], pgtable['name'].alias("movie_name"),\
            pgtable[ 'year'].alias("movie_year"),pgtable['score'].alias("movie_score"),\
            behaviorData['event_type'], behaviorData['event_payload'],\
            behaviorData['timestamp'])

#This dataframe aggregatese the watch percentage or average rating
#of each user
userGrouped = behaviorData.groupBy('user_id', 'event_type')\
                .agg(F.avg('event_payload').alias("avg_payload"))

#This dataframe aggregatese the watch percentage or average rating
#of each movie
movieGrouped = behaviorData.groupBy('movie_id', 'event_type')\
                .agg(F.avg('event_payload').alias("avg_payload"))

movieGrouped = movieGrouped.join(F.broadcast(pgtable), pgtable.id == movieGrouped.movie_id)\
    .select(pgtable['name'], movieGrouped['event_type'], movieGrouped['avg_payload'],\
            pgtable['score'], pgtable[ 'year'])

writeToPostgres(allJoinedoinedDF, url=configs["url"], dbtable="alljoined_batch",\
                user=configs["user"], password=configs["password"])

writeToPostgres(userGrouped, url=configs["url"], dbtable="usergrouped_batch",\
                user=configs["user"], password=configs["password"])

writeToPostgres(movieGrouped, url=configs["url"], dbtable="moviegrouped_batch",\
                user=configs["user"], password=configs["password"])