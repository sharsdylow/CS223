import psycopg
import time
from multiprocessing import Pool, Process, Lock
from math import ceil, floor
from psycopg import Connection

DROP = "dataset/schema/drop.sql"
CREAT = "dataset/schema/create.sql"
METADATA_LOW = "dataset-processed/metadata_low_concurrency.sql"
QUERIES_LOW = "dataset-processed/queries_low_concurrency.sql"
EXAMPLE = "dataset-example/example.sql"

TRANSACTION_SIZE = 100
THREADS_NUM = 4  # Multi Processing Level

CONNECTION_STRING = "host=localhost port=55432 dbname=postgres user = postgres password=example connect_timeout=10"


def execute_sql(filename: str):
    # Connect to an existing database
    with open(filename, "r") as fd:
        sqlCommands = fd.read().split(";")

    with psycopg.connect(CONNECTION_STRING) as conn:
        with conn.cursor() as cur:
            for command in sqlCommands:
                cur.execute(command)
            conn.commit()

def worker(thread_id, thread_queries):
    count, executed = 0, 0
    total = ceil(len(thread_queries)/TRANSACTION_SIZE)
    conn = psycopg.connect(CONNECTION_STRING)
    print(f"Thread {thread_id} connected to the database")
    try:
        with conn.cursor() as cursor:
            for query in thread_queries:
                if query.strip():
                    cursor.execute(query)
                    executed += 1
                if executed == TRANSACTION_SIZE:
                    conn.commit()
                    executed = 0
                    count += 1
                    print(f"Thread {thread_id} executed a transaction ({count}/{total})")
    except psycopg.Error as e:
        conn.rollback()
        print(f"Error executing transaction in thread {thread_id}: {e}")         
    conn.close()
    print(f"Thread {thread_id} disconnected from the database")

def execute_queries(filename: str):
    processes = []

    with open(filename) as sql_file:
        queries = sql_file.read().split(";")
        # init every thread
        for thread_id in range(THREADS_NUM):
            thread_queries = []
            start = thread_id * TRANSACTION_SIZE
            end = start + TRANSACTION_SIZE
            while start < len(queries):
                thread_queries += queries[start:end]    # get queries for each thread
                start += THREADS_NUM * TRANSACTION_SIZE
                end += THREADS_NUM * TRANSACTION_SIZE

            p = Process(target=worker, args=(thread_id, thread_queries))
            processes.append(p)
            p.start()

    for process in processes:
        process.join()

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
    meta_t = time.time()
    print(f"Metadata runtime: {meta_t-start}")

    # execute queries
    execute_queries(EXAMPLE)
    # execute_queries(EXAMPLE)
    # execute_sql(QUERIES_LOW, CONNECTION)
    print(f"Queries runtime: {time.time()-meta_t}")


if __name__ == "__main__":
    main()
