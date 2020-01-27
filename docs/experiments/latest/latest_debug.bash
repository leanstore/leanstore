#!/bin/bash -f

set -x
set -e

EXEC_DIR=../../../release/frontend/
function benchmark() {
    CSV_PATH="$(pwd)/results.csv"
    rm -f $CSV_PATH
    (cd $EXEC_DIR; make -j latest)

    for FLAGS_worker_threads in 120; do
    for FLAGS_latest_read_ratio in 0 25; do
    for FLAGS_zipf_factor in 0.80; do
    for FLAGS_target_gib in 11; do # 25 50 75
    for FLAGS_latest_window_gib in 1; do
    for FLAGS_latest_window_offset_gib in 0.1; do
    for FLAGS_latest_window_ms in 1000; do
    for FLAGS_dram_gib in 2.02 2.03; do
    for FLAGS_contention_update_tracker_pct in 1; do
    for FLAGS_contention_management in false; do
    for FLAGS_space_utilization in false; do
        (
        $EXEC_DIR/latest \
            -worker_threads=$FLAGS_worker_threads \
            -zipf_factor=$FLAGS_zipf_factor \
            -dram_gib=$FLAGS_dram_gib \
            -target_gib=$FLAGS_target_gib \
        -csv_path=$CSV_PATH \
        -nocsv_truncate \
        -ssd_path="/dev/md0" \
        -run_for_seconds=20 \
        -pp_threads=1 \
        -cool_pct=10 \
        -free_pct=1 \
        -latest_read_ratio=$FLAGS_latest_read_ratio \
        -latest_window_gib=$FLAGS_latest_window_gib \
        -latest_window_offset_gib=$FLAGS_latest_window_offset_gib \
        -latest_window_ms=$FLAGS_latest_window_ms \
        -contention_management=$FLAGS_contention_management \
        -space_utilization=$FLAGS_space_utilization \
        -contention_update_tracker_pct=$FLAGS_contention_update_tracker_pct \
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
    done
    done
}

benchmark

exit 0
