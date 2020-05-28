#!/bin/bash -f

set -x
set -e

EXEC_DIR=../../../../release/frontend/
EXEC_NAME="contention"
CSV_PATH="$(pwd)/${CSV_NAME}.csv"
function benchmark() {

    rm -f $CSV_PATH
    (cd $EXEC_DIR; make -j $EXEC_NAME)

    for FLAGS_worker_threads in 10; do # 30 60 90 120
    for FLAGS_cm_update_tracker_pct in 1; do # 1 10
    for FLAGS_cm_threads_pro_page in 1 2 3 4 5 6 8; do
    for FLAGS_su_merge in true false; do
    for FLAGS_cm_split in true false; do
        (
        $EXEC_DIR/$EXEC_NAME \
            -worker_threads=$FLAGS_worker_threads \
            -dram_gib=10 \
            -target_gib=1 \
        -csv_path=$CSV_PATH \
        -nocsv_truncate \
        -ssd_path="${SSD_PATH}" \
        -run_for_seconds=60 \
        -pp_threads=1 \
        -cool_pct=10 \
        -free_pct=1 \
        -cm_threads_pro_page=$FLAGS_cm_threads_pro_page \
        -cm_split=$FLAGS_cm_split \
        -cm_merge=$FLAGS_cm_split \
        -su_merge=$FLAGS_su_merge \
        -cm_update_tracker_pct=$FLAGS_cm_update_tracker_pct
    )

    done
    done
    done
    done
    done
}

benchmark

exit 0
