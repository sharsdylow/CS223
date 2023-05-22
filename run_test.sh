#!/bin/bash

VERBOSE=1

DIR_LOG="logs"
LOG_FILE_LOW="$DIR_LOG/low-$(date --iso-8601=seconds).log"
LOG_FILE_HIGH="$DIR_LOG/high-$(date --iso-8601=seconds).log"

DIR_PROCESSED="dataset-processed"
METADATA_LOW="${DIR_PROCESSED}/metadata_low_concurrency.sql"
QUERY_LOW="${DIR_PROCESSED}/queries_low_concurrency.sql"

METADATA_HIGH="${DIR_PROCESSED}/metadata_high_concurrency.sql"
QUERY_HIGH="${DIR_PROCESSED}/queries_high_concurrency.sql"

# Test conditions
MPL=(1 4 16 128) # Multi programming level
TRANSACTION_SIZE=(4 16 128 1024)
CONSISTENCY_LEVEL=(1 2 3 4)

# Create empty file
mkdir -p $DIR_LOG
touch "$LOG_FILE_LOW"
touch "$LOG_FILE_HIGH"
echo -e "level\tmpl\tsize\tthroughput\tquery\tworkload" >>"$LOG_FILE_LOW"
echo -e "level\tmpl\tsize\tthroughput\tquery\tworkload" >>"$LOG_FILE_HIGH"

# # Run tests on low concurrency dataset
# for lvl in "${CONSISTENCY_LEVEL[@]}"; do
#     for mpl in "${MPL[@]}"; do
#         for size in "${TRANSACTION_SIZE[@]}"; do
#             if [[ VERBOSE -eq 1 ]]; then
#                 echo "Running test under setup: CONSISTENCY_LEVEL=$lvl MPL=$mpl TRANSACTION_SIZE=$size"
#                 cmd="python main.py --consistency-level $lvl --size $size --workers $mpl --metadata $METADATA_LOW --queries $QUERY_LOW"
#                 echo $cmd
#                 echo -e -n "$lvl\t$mpl\t$size\t" >>"$LOG_FILE_LOW"
#                 $cmd | tail -n 1 >>"$LOG_FILE_LOW"
#             fi
#         done
#     done
# done

# Run tests on low concurrency dataset
for lvl in "${CONSISTENCY_LEVEL[@]}"; do
    for mpl in "${MPL[@]}"; do
        for size in "${TRANSACTION_SIZE[@]}"; do
            if [[ VERBOSE -eq 1 ]]; then
                echo "Running test under setup: CONSISTENCY_LEVEL=$lvl MPL=$mpl TRANSACTION_SIZE=$size"
                cmd="python main.py --consistency-level $lvl --size $size --workers $mpl --metadata $METADATA_HIGH --queries $QUERY_HIGH"
                echo $cmd
                echo -e -n "$lvl\t$mpl\t$size\t" >>"$LOG_FILE_HIGH"
                $cmd | tail -n 1 >>"$LOG_FILE_HIGH"
            fi
        done
    done
done
