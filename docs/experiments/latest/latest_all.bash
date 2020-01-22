#!/bin/bash -f

set -x
set -e

EXEC_DIR=../../../release/frontend/
function benchmark() {
    CSV_PATH="$(pwd)/results.csv"
    rm -f $CSV_PATH
    (cd $EXEC_DIR; make -j latest)

    for FLAGS_contention_management in false true; do
    for FLAGS_worker_threads in 120; do
    for FLAGS_latest_read_ratio in 0 25; do
    for FLAGS_zipf_factor in 0.75; do #0.80 0.85 0.90 0.95 0.99
    for FLAGS_dram_gib in 10; do
    for FLAGS_target_gib in 50; do # 25 50 75
    for FLAGS_latest_window_gib in 1; do
    for FLAGS_latest_window_offset_gib in 0.1; do
    for FLAGS_latest_window_ms in 1000 5000 10000; do
        (
        $EXEC_DIR/latest \
            -worker_threads=$FLAGS_worker_threads \
            -zipf_factor=$FLAGS_zipf_factor \
            -dram_gib=$FLAGS_dram_gib \
            -target_gib=$FLAGS_target_gib \
        -csv_path=$CSV_PATH \
        -nocsv_truncate \
        -ssd_path="/dev/md0" \
        -run_for_seconds=60 \
        -cool_pct=1 \
        -free_pct=1 \
        -latest_read_ratio=$FLAGS_latest_read_ratio \
        -latest_window_gib=$FLAGS_latest_window_gib \
        -latest_window_offset_gib=$FLAGS_latest_window_offset_gib \
        -latest_window_ms=$FLAGS_latest_window_ms \
        -contention_management=$FLAGS_contention_management
    )

    done
    done
    done
    done
    done
    done
    done
    done
    done
}

benchmark

exit 0
