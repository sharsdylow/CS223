# Preprocess the dataset and sort in time order
from datetime import datetime
import heapq

# FILENAME = "dataset-example/queries-example.txt"
FILENAME = "dataset/queries/low_concurrency/queries.txt"
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


def main():
    queries_list = []
    with open(FILENAME, "r") as f:
        sql_buffer = ""
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
                heapq.heappush(queries_list, (ts_float, sql_buffer))
                sql_buffer = ""
            else:
                sql_buffer += line.strip("\n").strip("\t").strip(" ") + " "
    return queries_list


if __name__ == "__main__":
    ret = main()
    while ret:
        q = heapq.heappop(ret)
        print(q)
