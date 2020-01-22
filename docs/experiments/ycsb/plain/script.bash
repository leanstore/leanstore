#!/bin/bash -f

set -x
set -e

EXEC_DIR=../../../release/frontend/
function benchmark() {
    CSV_PATH="$(pwd)/../csv/ycsb.csv"
    # config and run
    for FLAGS_contention_management in false true; do
    for FLAGS_worker_threads in 60; do
    for FLAGS_target_gib in 100; do
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
