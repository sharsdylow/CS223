# Preprocess the dataset and sort in time order
from datetime import datetime
from pathlib import Path
import heapq

# Low Concurrency dataset
QUERIES_LOW = "dataset/queries/low_concurrency/queries.txt"
OBS_LOW = "dataset/data/low_concurrency/observation_low_concurrency.sql"
SEM_OBS_LOW = "dataset/data/low_concurrency/semantic_observation_low_concurrency.sql"
META_LOW = "dataset/data/low_concurrency/metadata.sql"
SAVE_META_LOW = "dataset-processed/metadata_low_concurrency.sql"
SAVE_QUERIES_LOW = "dataset-processed/queries_low_concurrency.sql"


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


def process_metadata(read_file, save_file):
    with open(read_file, "r") as f:
        with open(save_file, "w") as wf:
            for line in f:
                if line.startswith("--"):
                    continue
                else:
                    wf.write(line)
            wf.close()


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
                set_queries.append((-1.0, line))
            elif line.startswith("INSERT"):
                start_of_inserts = idx  # Record start of INSERTs
                break

        f.seek(0, 0)  # Go back to start of file
        for _ in range(start_of_inserts):
            next(f)  # Skip until start of INSERTS

        for line in f:
            try:
                ts_iso = line.split("'")[-4] + "Z"
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
                current_query = current_query.strip(" ") + ";\n"
                heapq.heappush(queries_list, (ts_float, current_query))
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

    # Process low concurrency obs
    obs = process_sql(filename=OBS_LOW)

    # Process low concurrency sem-obs with transactions before
    sem_obs = process_sql(filename=SEM_OBS_LOW)

    # merge all queries
    all_queries = list(heapq.merge(queries, obs, sem_obs))

    # save all the transactions into one sql file
    save_queries(queries=all_queries, save_file=SAVE_QUERIES_LOW)

    # remove comments in metadata.sql
    process_metadata(META_LOW, SAVE_META_LOW)


if __name__ == "__main__":
    main()
