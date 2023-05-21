import psycopg
import time
from multiprocessing import Manager, Process
from math import ceil, floor
from psycopg import Connection

DROP = "dataset/schema/drop.sql"
CREAT = "dataset/schema/create.sql"
METADATA_LOW = "dataset-processed/metadata_low_concurrency.sql"
QUERIES_LOW = "dataset-processed/queries_low_concurrency.sql"
EXAMPLE = "dataset-example/example.sql"

TRANSACTION_SIZE = 64
THREADS_NUM = 100 # Multi Processing Level

CONNECTION_STRING = "host=localhost port=55432 dbname=postgres user = postgres password=example connect_timeout=10"


def execute_sql(filename: str):
    with open(filename, "r") as fd:
        sqlCommands = fd.read().split(";")

    with psycopg.connect(CONNECTION_STRING) as conn:
        with conn.cursor() as cur:
            for command in sqlCommands:
                cur.execute(command)
            conn.commit()

def worker(thread_id, queries, avg_response_time):
    conn = psycopg.connect(CONNECTION_STRING)
    print(f"Thread {thread_id} connected to the database")

    count = 0
    total = ceil(len(queries)/(TRANSACTION_SIZE * THREADS_NUM))
    start = thread_id * TRANSACTION_SIZE
    end = start + TRANSACTION_SIZE
    # executed, avg = 0, 0 # init number of executed queries and average response time.

    total_response_time = 0.0

    while start < len(queries):
        transaction_queries = queries[start:end]    # get queries for each thread
        try:
            with conn.cursor() as cursor:
                for query in transaction_queries:
                    if query.strip():
                        # last_avg = avg
                        # total_t = last_avg * executed

                        start_t = time.time()
                        cursor.execute(query)
                        end_t = time.time()

                        total_response_time += (end_t - start_t)
                        
                        # total_t += (end_t - start_t)
                        # executed += 1
                        # avg = total_t / executed                       
                conn.commit()
                count += 1 
        except psycopg.Error as e:
            conn.rollback()
            print(f"Error executing transaction in thread {thread_id}: {e}")  
        else:
            print(f"Thread {thread_id} executed a transaction ({count}/{total})")  

        start += THREADS_NUM * TRANSACTION_SIZE
        end += THREADS_NUM * TRANSACTION_SIZE   

    conn.close()
    # avg_response_time[thread_id] = avg
    avg_response_time[thread_id] = total_response_time / len(queries)
    print(f"Thread {thread_id} disconnected from the database")

def execute_queries(filename: str):
    start = time.time()
    processes = []
    avg_response_time = Manager().list([0 for _ in range(THREADS_NUM)])

    with open(filename) as sql_file:
        queries = Manager().list()
        queries += sql_file.read().split(";")
        # init every thread
        for thread_id in range(THREADS_NUM):
            p = Process(target=worker, args=(thread_id, queries, avg_response_time))
            processes.append(p)
            p.start()

    for process in processes:
        process.join()

    response_time = time.time() - start
    print(f"Response time of the whole workload: {response_time:.4f} seconds.")

    transaction_num = ceil(len(queries) / TRANSACTION_SIZE)
    throughput = transaction_num / response_time
    print(f"Throughput: {throughput:.4f} transactions per second.")

    used_workers = len([w for w in avg_response_time if w != 0.0])
    print(f"{used_workers} workers used")
    avg_queries_time = sum(avg_response_time) / used_workers
    avg_queries_time_1 = response_time / len(queries)
    print(f"Average response time for queries: {avg_queries_time:.4f} seconds.")
    print(f"Average response time for queries: {avg_queries_time_1:.4f} seconds.")




def main():
    start = time.time()

    # Init database
    execute_sql(DROP)
    print("Delete all tables successfully.")
    # Create table
    execute_sql(CREAT)
    print("Create all tables successfully.")
    # Insert metadata
    execute_sql(METADATA_LOW)
    print("Insert all metadata successfully.")
    
    # execute queries
    execute_queries(QUERIES_LOW)
    # execute_queries(EXAMPLE)




if __name__ == "__main__":
    main()
