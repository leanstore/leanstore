#!/bin/bash -f

# Measure CM overhead with YCSB (it should not split)
set -x
set -e

EXEC_DIR=../../../../release/frontend/
function benchmark() {
    CSV_PATH="$(pwd)/results.csv"
    rm -f $CSV_PATH
   (cd $EXEC_DIR; make -j ycsb)

    # config and run
    for FLAGS_cm_split in false true; do
    for FLAGS_worker_threads in 120; do
    for FLAGS_ycsb_read_ratio in 0 25 50; do
    for FLAGS_target_gib in 100; do
    for FLAGS_dram_gib in 200; do
    for FLAGS_zipf_factor in 0.75 0.90; do
    for FLAGS_run_for_seconds in 60; do
    (
        $EXEC_DIR/ycsb \
            -worker_threads=$FLAGS_worker_threads \
            -ycsb_read_ratio=$FLAGS_ycsb_read_ratio \
            -zipf_factor=$FLAGS_zipf_factor \
            -dram_gib=$FLAGS_dram_gib \
            -target_gib=$FLAGS_target_gib \
            -csv_path=$CSV_PATH \
            -ssd_path="/dev/md0" \
            -run_for_seconds=$FLAGS_run_for_seconds \
            -cool_pct=1 \
            -free_pct=1 \
            -cm_split=$FLAGS_cm_split
    )

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
