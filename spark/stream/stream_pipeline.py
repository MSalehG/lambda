import pyspark.sql
from pyspark.sql import SparkSession
from pyspark import SparkConf
import pyspark.sql.functions as F
from json import loads

configs = loads(open("/job/configs.json", 'r').read().strip())["streaming"]
conf = SparkConf().setAppName("lambda_stream")\
    .setAll(configs["sparkConf"])
spark = SparkSession.Builder().master('local[1]')\
    .config(conf=conf).getOrCreate()

sc = spark.sparkContext
log4jLogger = sc._jvm.org.apache.log4j
log4jLogger.LogManager.getRootLogger().setLevel(log4jLogger.Level.WARN)

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

#In Structured streaming the only way to save a dataframe through a
#JDBC connection is to use a foreachBatch function on the dataframe
#and the function being used only accepts two fixed parameters
#So for three separate tables we wrote three separate functions
def writeAllJoined(dataframe:pyspark.sql.DataFrame, batchId:int) -> None:
    dataframe.show()
    dataframe.write.mode("append") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/my_database") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "alljoined_stream") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .save()

def writeUserGrouped(dataframe:pyspark.sql.DataFrame, batchId:int) -> None:
    dataframe.show()
    dataframe.write.mode("append") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/my_database") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "usergrouped_stream") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .save()

def writeMovieGrouped(dataframe:pyspark.sql.DataFrame, batchId:int) -> None:
    dataframe.write.mode("append") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/my_database") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "moviegrouped_stream") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .save()

df = spark\
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", configs["broker"]) \
    .option("subscribe", configs["topic"]) \
    .option("maxOffsetsPerTrigger", "20")\
    .option("startingOffsets", "earliest")\
    .option("failOnDataLoss", "false")\
    .load()

#Decode the byte array which is the actual record
behaviorData=df.withColumn("decoded", F.col("value").cast("string")).select(F.col("decoded"))
splitter = F.split(behaviorData['decoded'], ',')

behaviorData = behaviorData.withColumn("user_id", splitter.getItem(0).cast("int"))
behaviorData = behaviorData.withColumn("movie_id", splitter.getItem(1).cast("int"))
behaviorData = behaviorData.withColumn("event_type", splitter.getItem(2))
behaviorData = behaviorData.withColumn("event_payload", splitter.getItem(3).cast("int"))
behaviorData = behaviorData.withColumn("timestamp", splitter.getItem(4))
behaviorData = behaviorData.withColumn("timestamp", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
behaviorData = behaviorData.drop("decoded")
behaviorData = behaviorData.na.drop(subset=["user_id", "movie_id"])


#A query to fetch movie metadata in parallel in contain it in multiple partitions in spark
pgQuery = f"""(SELECT mod({configs['partitionColumn']}, 4) as partitionKey, * from movies) as subQuery"""
pgtable = readFromPostgres(spark, user=configs["user"], password=configs["password"],\
                           query=pgQuery, ptColumn="partitionKey",\
                           lowerBound=0, upperBound=f"{int(configs['numPartitions'])}", numPartitions=configs["numPartitions"]).cache()

# #This dataframe is the joined version of the untransformed data
allJoinedDF = behaviorData.join(F.broadcast(pgtable), pgtable.id == behaviorData.movie_id)\
    .select(behaviorData['user_id'], pgtable['name'].alias("movie_name"),\
            pgtable[ 'year'].alias("movie_year"),pgtable['score'].alias("movie_score"),\
            behaviorData['event_type'], behaviorData['event_payload'],\
            behaviorData['timestamp'])

#This dataframe aggregatese the watch percentage or average rating
#of each user
userGrouped = behaviorData\
    .withWatermark("timestamp", "20 seconds")\
    .groupBy('user_id', 'event_type', "timestamp")\
    .agg(F.avg('event_payload').alias("avg_payload"))

#This dataframe aggregatese the watch percentage or average rating
#of each movie
movieGrouped = behaviorData\
    .withWatermark("timestamp", "20 seconds")\
    .groupBy('movie_id', 'event_type', "timestamp")\
    .agg(F.avg('event_payload').alias("avg_payload"))

movieGrouped = movieGrouped.join(F.broadcast(pgtable), pgtable.id == movieGrouped.movie_id)\
    .select(pgtable['name'], movieGrouped['event_type'], movieGrouped['avg_payload'],\
            pgtable['score'], pgtable[ 'year'])


dsAllJoined = allJoinedDF.writeStream \
    .foreachBatch(writeAllJoined)\
    .trigger(processingTime='10 seconds')\
    .start()

dsUserGrouped = userGrouped.writeStream \
    .foreachBatch(writeUserGrouped)\
    .trigger(processingTime='10 seconds')\
    .start()

dsMovieGrouped = movieGrouped.writeStream \
    .foreachBatch(writeMovieGrouped)\
    .trigger(processingTime='10 seconds')\
    .start()

spark.streams.awaitAnyTermination()
