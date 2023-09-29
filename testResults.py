import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter, AsIs
import sys
import numpy as np

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

tableList = ["movies",
             "allJoined_batch", "userGrouped_batch", "movieGrouped_batch",\
             "allJoined_stream", "userGrouped_stream", "movieGrouped_stream"]



for table in tableList:
    query = f"SELECT * from {table}"
    records = postgresConnector(query=query)
    print(f"The number of records inside {table} is {len(records)}")