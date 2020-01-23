#!/bin/bash -f

set -x
set -e

EXEC_DIR=../../../release/frontend/
function benchmark() {
    CSV_PATH="$(pwd)/tpcc_60.csv"
    rm -f $CSV_PATH
    (cd $EXEC_DIR; make -j tpcc)

    for FLAGS_worker_threads in 60; do
    for FLAGS_target_gib in 150; do # 25 50 75
    for FLAGS_dram_gib in 200; do
    for FLAGS_tpcc_warehouse_count in 30 60 100 120; do
    for FLAGS_contention_management in false true; do
        (
        $EXEC_DIR/tpcc \
            -worker_threads=$FLAGS_worker_threads \
            -zipf_factor=0 \
            -dram_gib=$FLAGS_dram_gib \
            -csv_path=$CSV_PATH \
            -nocsv_truncate \
            -ssd_path="/dev/md0" \
            -run_for_seconds=30 \
            -cool_pct=20 \
            -free_pct=1 \
            -tpcc_warehouse_count=$FLAGS_tpcc_warehouse_count \
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
