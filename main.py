import psycopg
import time
from multiprocessing import Manager, Process
from math import ceil, floor
from psycopg import Connection
import argparse

DROP = "dataset/schema/drop.sql"
CREAT = "dataset/schema/create.sql"
METADATA = "dataset-processed/metadata_low_concurrency.sql"
QUERIES = "dataset-processed/queries_low_concurrency.sql"
EXAMPLE = "dataset-example/example.sql"

TRANSACTION_SIZE = 64
THREADS_NUM = 4  # Multi Processing Level
# fmt: off
CONSISTANCY_LEVEL = 4  # READ_UNCOMMITTED = 1 READ_COMMITTED = 2 REPEATABLE_READ = 3 SERIALIZABLE = 4
# fmt: on
TRUNCATE = 0

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
    # set consistency level
    conn.isolation_level = CONSISTANCY_LEVEL
    # init committed number of transactions and number of executed queries
    transaction_count = 0
    query_count = 0
    total_response_time = 0.0
    # calculate number of total transactions
    total = ceil(len(queries) / TRANSACTION_SIZE)

    try:
        with conn.cursor() as cursor:
            for query in queries:
                if query.strip():
                    start_t = time.time()
                    cursor.execute(query)
                    end_t = time.time()
                    query_count += 1
                    total_response_time += end_t - start_t

                if query_count == TRANSACTION_SIZE:
                    conn.commit()
                    transaction_count += 1
                    query_count = 0
                    print(
                        f"Thread {thread_id} executed a transaction ({transaction_count}/{total})"
                    )

    except psycopg.Error as e:
        conn.rollback()
        print(f"Error executing transaction in thread {thread_id}: {e}")

    conn.close()
    # avg_response_time[thread_id] = avg
    avg_response_time[thread_id] = total_response_time / len(queries)
    print(f"Thread {thread_id} disconnected from the database")


def execute_sql_concurrent(filename: str, truncate_lines: int = 0):
    start_t = time.time()
    processes = []
    avg_response_time = Manager().list([0 for _ in range(THREADS_NUM)])

    with open(filename) as sql_file:
        queries = sql_file.read().split(";")
        if truncate_lines:
            queries = queries[:truncate_lines]

        query_slices = [[] for _ in range(THREADS_NUM)]
        # init every thread
        for thread_id in range(THREADS_NUM):
            start = thread_id * TRANSACTION_SIZE
            end = start + TRANSACTION_SIZE
            while start < len(queries):
                # queries_slice += queries[start:end]    # get queries for each thread
                query_slices[thread_id] += queries[
                    start:end
                ]  # get queries for each thread
                start += THREADS_NUM * TRANSACTION_SIZE
                end += THREADS_NUM * TRANSACTION_SIZE

            if len(query_slices[thread_id]) == 0:
                continue  # Do not initiate worker if its query workload is empty

            p = Process(
                target=worker,
                args=(thread_id, query_slices[thread_id], avg_response_time),
            )
            processes.append(p)
            p.start()

    for process in processes:
        process.join()

    workload_time = time.time() - start_t
    print(f"Response time of the whole workload: {workload_time:.4f} seconds.")

    transaction_num = ceil(len(queries) / TRANSACTION_SIZE)
    throughput = transaction_num / workload_time
    print(f"Throughput: {throughput:.4f} transactions per second.")

    used_workers = len([w for w in avg_response_time if w != 0.0])
    print(f"{used_workers} workers used")
    avg_query_time = sum(avg_response_time) / used_workers
    print(f"Average response time for queries: {avg_query_time:.8f} seconds.")

    return (throughput, avg_query_time, workload_time)


def main():
    # Init database
    execute_sql(DROP)
    print("Delete all tables successfully.")
    # Create table
    execute_sql(CREAT)
    print("Create all tables successfully.")
    # Insert metadata
    execute_sql(METADATA)
    print("Insert all metadata successfully.")

    # execute queries
    (throughput, avg_query_time, workload_time) = execute_sql_concurrent(
        QUERIES, TRUNCATE
    )
    print("Report: ")
    print(f"Throughput\tQuery\tWorkload")
    print(f"{throughput}\t{avg_query_time}\t{workload_time}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Test concurrent transactions with PostgreSQL. "
    )
    parser.add_argument(
        "--size", metavar="T", type=int, help="Transaction size", default=16
    )
    parser.add_argument(
        "--workers",
        metavar="W",
        type=int,
        help="Number of workers (client threads)",
        default=64,
    )
    parser.add_argument(
        "--truncate",
        type=int,
        help="Truncate input query file to N lines",
    )
    parser.add_argument(
        "--metadata",
        type=str,
        help="A file containing all database metadata",
        required=True,
    )
    parser.add_argument(
        "--queries",
        type=str,
        help="A file containing all database queries",
        required=True,
    )
    parser.add_argument(
        "--consistency-level",
        type=int,
        help="Consistency level (1~4)",
        default=2,
    )

    args = parser.parse_args()
    TRANSACTION_SIZE = args.size
    THREADS_NUM = args.workers
    TRUNCATE = args.truncate
    METADATA = args.metadata
    QUERIES = args.queries
    CONSISTANCY_LEVEL = args.consistency_level

    # print(args)

    main()
