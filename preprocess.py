# Preprocess the dataset and sort in time order
import heapq

# FILENAME = "dataset-example/queries-example.txt"
FILENAME = "dataset/queries/low_concurrency/queries.txt"


def scale_ts_to_float(timestamp: str):
    return 0.0


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
                ts = line[:-2]
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
