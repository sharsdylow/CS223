# Preprocess the dataset and sort in time order
from datetime import datetime
from pathlib import Path
import heapq

QUERIES_LOW = "dataset/queries/low_concurrency/queries.txt"
SAVE_QUERIES_LOW = "dataset-processed/queries.txt"

# Low Concurrency dataset
OBS_LOW = "dataset/data/low_concurrency/observation_low_concurrency.sql"
SAVE_OBS_LOW = "dataset-processed/observation_low_concurrency.sql"
SEM_OBS_LOW = "dataset/data/low_concurrency/semantic_observation_low_concurrency.sql"
SAVE_SEM_OBS_LOW = "dataset-processed/semantic_observation_low_concurrency.sql"
METADATA_LOW = "dataset/data/low_concurrency/metadata.sql"


DEFAULT_SCALE = 1440  # Compress 20 days -> 20 minutes
TIMESTAMP_BASE = 1510099200  # Epoch time of 2017-11-08T00:00:00Z


def scale_ts_to_float(ts: str, scale=DEFAULT_SCALE, ts_base=TIMESTAMP_BASE) -> float:
    """
    input: timestamp (str), precisely in the format of YYYY-MM-DDTHH:MM:SSZ
    output: scaled timestamp in float (float)
    """
    ts_int = datetime.fromisoformat(ts).timestamp()
    ts_float = (ts_int - ts_base) / scale

    return ts_float


def process_sql(filename: str):
    queries_list = []
    set_queries = []  # Store the SET queries and later append to start of queries_list
    with open(filename, "r") as f:
        current_query = ""
        ts = ""
        ts_float = 0.0
        start_of_inserts = 0

        # Preserve the SET lines at the start
        # Perform an initial scan for the start of INSERT lines
        for idx, line in enumerate(f):
            if line.startswith("SET"):
                set_queries.append((0, line))
            elif line.startswith("INSERT"):
                start_of_inserts = idx  # Record start of INSERTs
                break

        f.seek(0, 0)  # Go back to start of file
        for _ in range(start_of_inserts):
            next(f)  # Skip until start of INSERTS

        for line in f:
            try:
                ts_iso = line.split("'")[-4]
            except IndexError:
                continue
            ts_float = scale_ts_to_float(ts_iso)
            heapq.heappush(queries_list, (ts_float, line))

        queries_list = set_queries + queries_list
    return queries_list


def process_queries(filename):
    queries_list = []
    with open(filename, "r") as f:
        current_query = ""
        ts = ""
        ts_float = 0.0

        for line in f:
            if ',"' in line:
                # This line is a timestamp
                # in ISO 8601 format:
                # YYYY-MM-DDTHH:MM:SSZ,
                ts = line[:-3]
                ts_float = scale_ts_to_float(ts)
            elif '"' in line:
                heapq.heappush(queries_list, (ts_float, current_query + "\n"))
                current_query = ""
            else:
                current_query += line.strip("\n").strip("\t").strip(" ") + " "
    return queries_list


def save_queries(queries: list[tuple[float, str]], save_file: str):
    with open(save_file, "w") as f:
        while queries:
            q = heapq.heappop(queries)
            f.write(q[1])
            # f.write("\n")
        f.close()


def main():
    # Create dataset-processed directory if not existing
    Path("./dataset-processed").mkdir(parents=True, exist_ok=True)

    # Process low concurrency queries
    queries = process_queries(filename=QUERIES_LOW)
    save_queries(queries=queries, save_file=SAVE_QUERIES_LOW)

    # Process low concurrency obs
    obs = process_sql(filename=OBS_LOW)
    save_queries(queries=obs, save_file=SAVE_OBS_LOW)

    # Process low concurrency sem-obs
    sem_obs = process_sql(filename=SEM_OBS_LOW)
    save_queries(queries=sem_obs, save_file=SAVE_SEM_OBS_LOW)


if __name__ == "__main__":
    main()
