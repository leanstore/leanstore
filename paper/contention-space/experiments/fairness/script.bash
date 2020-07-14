#!/bin/bash -f

set -x
set -e
EXEC_DIR=../../../../release/frontend/
EXEC_NAME="fairness"
CSV_PATH="$(pwd)/fairness"

    rm -f $CSV_PATH*.csv
    (cd $EXEC_DIR; make -j $EXEC_NAME)

    for FLAGS_worker_threads in 64 126; do # 10 20 30 40 50 60 70 80 90 100 110 120
    for FLAGS_mutex in false true; do
    for FLAGS_pin_threads in true; do
    for FLAGS_smt in true; do
    for FLAGS_backoff in 0 512; do
        (
        $EXEC_DIR/$EXEC_NAME \
        -worker_threads=$FLAGS_worker_threads \
        -csv_path=$CSV_PATH \
        -mutex=$FLAGS_mutex \
        -backoff=$FLAGS_backoff \
        -pp_threads=0 \
        -pin_threads=$FLAGS_pin_threads \
        -smt=$FLAGS_smt \
        -run_for_seconds=10
    )

    done
    done
    done
    done
    done
