#!/bin/bash -f

set -x
set -e
EXEC_DIR=../../../../release/frontend/
EXEC_NAME="seq"
CSV_PATH="$(pwd)/${PLATFORM}.csv"

    #rm -f $CSV_PATH
    (cd $EXEC_DIR; make -j $EXEC_NAME)

    for FLAGS_worker_threads in 10 15 20; do # 10 20 30 40 50 60 70 80 90 100 110 120
    for FLAGS_mutex in false true; do
    for FLAGS_x in 0 512; do
        (
        $EXEC_DIR/$EXEC_NAME \
            -worker_threads=$FLAGS_worker_threads \
        -csv_path=$CSV_PATH \
        -mutex=$FLAGS_mutex \
        -x=$FLAGS_x \
        -run_for_seconds=10
    )

    done
    done
    done
