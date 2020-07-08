#!/bin/bash -f

set -x
set -e

EXEC_DIR=../../../../release/frontend/
EXEC_NAME="latest"
function benchmark() {
    CSV_PATH="$(pwd)/latest"
#    rm -f $CSV_PATH*.csv
    (cd $EXEC_DIR; make -j $EXEC_NAME)

    for FLAGS_worker_threads in 8; do
    for FLAGS_latest_read_ratio in 0; do
    for FLAGS_zipf_factor in 0.90; do
    for FLAGS_dram_gib in 2; do
    for FLAGS_target_gib in 10; do # 25 50 75
    for FLAGS_latest_window_gib in 1; do
    for FLAGS_latest_window_offset_gib in 0.1; do
    for FLAGS_latest_window_ms in 10000; do
    for FLAGS_cm_update_tracker_pct in 1; do
    for FLAGS_cm_split in false true; do
        (
        $EXEC_DIR/latest \
            -worker_threads=$FLAGS_worker_threads \
            -zipf_factor=$FLAGS_zipf_factor \
            -dram_gib=$FLAGS_dram_gib \
            -target_gib=$FLAGS_target_gib \
        -csv_path=$CSV_PATH \
        -nocsv_truncate \
        -ssd_path="${SSD_PATH}" \
        -run_for_seconds=30 \
        -pp_threads=4 \
        -cool_pct=10 \
        -free_pct=1 \
        -bulk_insert \
        -latest_read_ratio=$FLAGS_latest_read_ratio \
        -latest_window_gib=$FLAGS_latest_window_gib \
        -latest_window_offset_gib=$FLAGS_latest_window_offset_gib \
        -latest_window_ms=$FLAGS_latest_window_ms \
        -cm_split=$FLAGS_cm_split \
        -su_merge=true #$FLAGS_cm_split
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
