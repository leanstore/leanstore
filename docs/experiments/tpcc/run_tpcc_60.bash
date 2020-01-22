#!/bin/bash -f

set -x
set -e

EXEC_DIR=../../../release/frontend/
function benchmark() {
    CSV_PATH="$(pwd)/results.csv"
    rm -f $CSV_PATH
    (cd $EXEC_DIR; make -j tpcc)

    for FLAGS_worker_threads in 56; do
    for FLAGS_target_gib in 150; do # 25 50 75
    for FLAGS_dram_gib in 200 201 202; do
    for FLAGS_tpcc_warehouse_count in 1 10 100 200 300; do
    for FLAGS_contention_management in false true; do
    for FLAGS_backoff_strategy in 0 1 2; do
        (
        $EXEC_DIR/tpcc \
            -worker_threads=$FLAGS_worker_threads \
            -zipf_factor=0 \
            -dram_gib=$FLAGS_dram_gib \
            -csv_path=$CSV_PATH \
            -nocsv_truncate \
            -ssd_path="/dev/md0" \
            -run_for_seconds=20 \
            -cool_pct=1 \
            -free_pct=1 \
            -tpcc_warehouse_count=$FLAGS_tpcc_warehouse_count \
            -contention_management=$FLAGS_contention_management
            -backoff_strategy=$FLAGS_backoff_strategy
    )

    done
    done
    done
    done
    done
    done
}

benchmark

exit 0

    CSV_PATH="$(pwd)/tpcc_60.csv"
    # config and run
    for FLAGS_contention_management in false true; do
    for FLAGS_worker_threads in 60; do
    for FLAGS_tpcc_warehouse_count in 1 10 100 200 300; do
    for FLAGS_dram_gib in 200; do
    for FLAGS_run_for_seconds in 20; do
    (
        $EXEC_DIR/tpcc \
            -worker_threads=$FLAGS_worker_threads \
            -tpcc_warehouse_count=$FLAGS_tpcc_warehouse_count \
            -dram_gib=$FLAGS_dram_gib \
        -csv_path=$CSV_PATH \
        -ssd_path="/dev/md0" \
        -run_for_seconds=$FLAGS_run_for_seconds \
        -cool_pct=20 \
        -free_pct=1 \
        -contention_management=$FLAGS_contention_management
    )

    done
    done
    done
    done
    done
}

benchmark

exit 0
