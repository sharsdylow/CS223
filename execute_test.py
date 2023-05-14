import psycopg

DROP = "dataset/schema/drop.sql"
CREAT = "dataset/schema/create.sql"

CONNECTION = "host=173.255.210.115 port=5432 dbname=mydb user = postgres password=example connect_timeout=10"

def execute_sql(filename: str, connection_string: str):
    # Connect to an existing database
    with psycopg.connect(connection_string) as conn:
        # Open a cursor to perform database operations
        with conn.cursor() as cur:
            fd = open(filename, 'r')
            sqlFile = fd.read()
            fd.close()
            sqlCommands = sqlFile.split(';')
            for command in sqlCommands:
                try:
                    cur.execute(command)
                except:
                    print("Command skipped")
            conn.commit()

def main():
    # print (psycopg.connect(CONNECTION))
    execute_sql(CREAT, CONNECTION)

if __name__== "__main__":
    main()