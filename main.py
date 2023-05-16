import psycopg
import time
import subprocess
from math import floor

DROP = "dataset/schema/drop.sql"
CREAT = "dataset/schema/create.sql"
METADATA_LOW = "dataset-processed/metadata_low_concurrency.sql"
QUERIES_LOW = "dataset-processed/queries_low_concurrency.sql"

TRANSACTION_SIZE = 2048

CONNECTION = "host=localhost port=55432 dbname=postgres user = postgres password=example connect_timeout=10"


def execute_sql(filename: str, connection_string: str):
    cnt = TRANSACTION_SIZE
    committed = 0
    # Connect to an existing database
    fd = open(filename, "r")
    sqlFile = fd.read()
    fd.close()
    sqlCommands = sqlFile.split(";")
    total_transactions = floor(len(sqlCommands) / TRANSACTION_SIZE)

    with psycopg.connect(connection_string) as conn:
        # Open a cursor to perform database operations
        with conn.cursor() as cur:
            for command in sqlCommands:
                try:
                    cur.execute(command)
                except:
                    print("Command skipped")
                    continue
                finally:
                    cnt -= 1
                if cnt == 0:
                    committed += 1
                    conn.commit()
                    print(f"Commit successful ({committed}/{total_transactions})")
                    cnt = TRANSACTION_SIZE


def main():
    start = time.time()
    execute_sql(DROP, CONNECTION)
    print("Delete all tables successfully.")
    # Init database
    # Create table
    execute_sql(CREAT, CONNECTION)
    print("Create all tables successfully.")
    # Insert metadata
    execute_sql(METADATA_LOW, CONNECTION)
    print(f"Metadata runtime: {time.time()-start}")

    execute_sql(QUERIES_LOW, CONNECTION)
    print(f"Queries runtime: {time.time()-start}")


if __name__ == "__main__":
    main()
