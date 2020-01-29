#!/bin/bash -f

set -x
set -e

EXEC_DIR=../../../release/frontend/
EXEC_NAME="contention"
function benchmark() {
    CSV_PATH="$(pwd)/results.csv"

    #rm -f $CSV_PATH
    (cd $EXEC_DIR; make -j $EXEC_NAME)

    for FLAGS_worker_threads in 120; do # 30 60 90 120
    for FLAGS_cm_update_tracker_pct in 1; do # 1 10
    for FLAGS_cm_threads_pro_page in 1; do
    for FLAGS_cm_split in false true; do
        (
        $EXEC_DIR/$EXEC_NAME \
            -worker_threads=$FLAGS_worker_threads \
            -dram_gib=10 \
            -target_gib=1 \
        -csv_path=$CSV_PATH \
        -nocsv_truncate \
        -ssd_path="/dev/md0" \
        -run_for_seconds=60 \
        -pp_threads=1 \
        -cool_pct=10 \
        -free_pct=1 \
        -cm_threads_pro_page=$FLAGS_cm_threads_pro_page \
        -cm_split=$FLAGS_cm_split \
        -cm_merge=$FLAGS_cm_split \
        -cm_update_tracker_pct=$FLAGS_cm_update_tracker_pct
    )

    done
    done
    done
    done
}

benchmark

exit 0
