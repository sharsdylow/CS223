import psycopg
import time
from multiprocessing import Pool, Process, Lock
from math import ceil, floor
from psycopg import Connection

DROP = "dataset/schema/drop.sql"
CREAT = "dataset/schema/create.sql"
METADATA_LOW = "dataset-processed/metadata_low_concurrency.sql"
QUERIES_LOW = "dataset-processed/queries_low_concurrency.sql"

TRANSACTION_SIZE = 16
THREADS = 4  # Multi Processing Level

CONNECTION = "host=localhost port=55432 dbname=postgres user = postgres password=example connect_timeout=10"


def execute_transaction(transaction):
    with psycopg.connect(CONNECTION) as conn:
        # Open a cursor to perform database operations
        with conn.cursor() as cur:
            print(conn.info.status)
            for query in transaction:
                # print(f"query={query[:64]}...")
                cur.execute(query)
        # conn.commit()


def execute_sql(filename: str, connection_string: str):
    # Connect to an existing database
    fd = open(filename, "r")
    sqlFile = fd.read()
    fd.close()
    sqlCommands = sqlFile.split(";")

    total_queries = len(sqlCommands)
    transactions = [
        (
            sqlCommands[i : i + TRANSACTION_SIZE]
            if i < floor(total_queries / TRANSACTION_SIZE) * TRANSACTION_SIZE
            else sqlCommands[i : i + total_queries % TRANSACTION_SIZE]
        )
        for i in range(0, total_queries, TRANSACTION_SIZE)
    ]
    total_transactions = len(transactions)

    # db_connections = [
    #     psycopg.connect(connection_string, autocommit=False) for _ in range(THREADS)
    # ]

    for batch in range(floor(total_transactions / THREADS)):
        processes = []
        for th in range(THREADS):
            curr_transaction = transactions[batch + th]
            processes.append(
                Process(
                    target=execute_transaction,
                    args=(curr_transaction,),
                )
            )

        for p in processes:
            p.start()

    # Last batch
    remaining = transactions[
        total_transactions - floor(total_transactions / THREADS) - 1 :
    ]
    for idx, t in enumerate(remaining):
        Process(
            target=execute_transaction,
            args=(t,),
        ).start()


def main():
    start = time.time()
    execute_sql(DROP, CONNECTION)
    print("Delete all tables successfully.")
    # Init database
    # Create table
    execute_sql(CREAT, CONNECTION)
    print("Create all tables successfully.")
    # Insert metadata
    # execute_sql(METADATA_LOW, CONNECTION)
    # print(f"Metadata runtime: {time.time()-start}")

    # execute_sql(QUERIES_LOW, CONNECTION)
    # print(f"Queries runtime: {time.time()-start}")


if __name__ == "__main__":
    main()
