#!/bin/bash -f

set -x
set -e

EXEC_DIR=../../../release/frontend/
function benchmark() {
    CSV_PATH="$(pwd)/results_overnight.csv"
    rm -f $CSV_PATH
    (cd $EXEC_DIR; make -j latest)

    for FLAGS_worker_threads in 120; do
    for FLAGS_latest_read_ratio in 0 25 50; do
    for FLAGS_zipf_factor in 0.80 0.99; do
    for FLAGS_dram_gib in 2 200; do
    for FLAGS_target_gib in 300; do # 25 50 75
    for FLAGS_latest_window_gib in 1 10; do
    for FLAGS_latest_window_offset_gib in 0.1 0.2 0.5; do
    for FLAGS_latest_window_ms in 1000 2000 5000 10000; do
    for FLAGS_contention_update_tracker_pct in 1; do
    for FLAGS_contention_management in true false; do
        (
        $EXEC_DIR/latest \
            -worker_threads=$FLAGS_worker_threads \
            -zipf_factor=$FLAGS_zipf_factor \
            -dram_gib=$FLAGS_dram_gib \
            -target_gib=$FLAGS_target_gib \
        -csv_path=$CSV_PATH \
        -nocsv_truncate \
        -ssd_path="/dev/md0" \
        -run_for_seconds=120 \
        -pp_threads=1 \
        -cool_pct=10 \
        -free_pct=1 \
        -latest_read_ratio=$FLAGS_latest_read_ratio \
        -latest_window_gib=$FLAGS_latest_window_gib \
        -latest_window_offset_gib=$FLAGS_latest_window_offset_gib \
        -latest_window_ms=$FLAGS_latest_window_ms \
        -contention_management=$FLAGS_contention_management \
        -space_utilization=$FLAGS_contention_management \
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
}

benchmark

exit 0

# + for FLAGS_contention_management in true false
# + ../../../release/frontend//latest -worker_threads=120 -zipf_factor=0.99 -dram_gib=2 -target_gib=300 -csv_path=/home/alhomssi/dev/leanstore/docs/experiments/latest/results_overnight.csv -nocsv_truncate -ssd_path=/dev/md0 -run_for_seconds=120 -pp_threads=1 -cool_pct=10 -free_pct=1 -latest_read_ratio=25 -latest_window_gib=1 -latest_window_offset_gib=0.2 -latest_window_ms=10000 -contention_management=false -space_utilization=false -contention_update_tracker_pct=1
# -------------------------------------------------------------------------------------
# Going out of memory !
