#!/bin/bash -f

set -x
set -e

EXEC_DIR=../../../release/frontend/
function benchmark() {
    export CSV_DIR="$(pwd)/../csv/tpcc"
    mkdir -p $CSV_DIR
    # config and run
    for FLAGS_contention_management in false true; do
    for FLAGS_worker_threads in 100; do
    for FLAGS_zipf_factor in 0.80 0.90; do
    for FLAGS_dram_gib in 200; do
    for FLAGS_run_for_seconds in 20; do
        FILE_SUFFIX="tpcc_skew_100_"$FLAGS_zipf_factor"_"$FLAGS_contention_management

    (
        $EXEC_DIR/tpcc \
            -worker_threads=$FLAGS_worker_threads \
            -tpcc_warehouse_count=100 \
            -dram_gib=$FLAGS_dram_gib \
        -file_suffix=$FILE_SUFFIX \
        -csv_dir=$CSV_DIR \
        -ssd_path="/dev/md0" \
        -run_for_seconds=$FLAGS_run_for_seconds \
        -contention_management=$FLAGS_contention_management \
        --zipf_factor=$FLAGS_zipf_factor \
        --notpcc_warehouse_affinity
    )

    done
    done
    done
    done
    done
}

benchmark

exit 0
