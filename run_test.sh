#!/bin/bash

VERBOSE=1

DIR_LOG="logs"
LOG_FILE="$DIR_LOG/db-test-$(date --iso-8601=seconds).log"

DIR_PROCESSED="dataset-processed"
METADATA_LOW="${DIR_PROCESSED}/metadata_low_concurrency.sql"
QUERY_LOW="${DIR_PROCESSED}/queries_low_concurrency.sql"

METADATA_HIGH="${DIR_PROCESSED}/metadata_high_concurrency.sql"
QUERY_HIGH="${DIR_PROCESSED}/queries_high_concurrency.sql"

# Test conditions
MPL=(1 4 16 128) # Multi programming level
TRANSACTION_SIZE=(4 16 128 1024)

# Create empty file
mkdir -p $DIR_LOG
touch "$LOG_FILE"

# Run tests on low concurrency dataset

for mpl in "${MPL[@]}"; do
    for size in "${TRANSACTION_SIZE[@]}"; do
        if [[ VERBOSE -eq 1 ]]; then
            echo "Running test under setup: MPL=$mpl TRANSACTION_SIZE=$size"
            cmd="python main.py --truncate 1000 --size $size --workers $mpl --metadata $METADATA_LOW --queries $QUERY_LOW"
            echo $cmd
            $cmd | tail -n 1 >> "$LOG_FILE"
        fi
    done
done