#!/bin/bash -f

set -x
set -e

EXEC_DIR=../../../../release/frontend/
EXEC_NAME="merge"

function benchmarkA() { # URLs
    CSV_PATH="$(pwd)/em_a"
    rm -f $CSV_PATH
    rm -f 'merge.csv'
    (cd $EXEC_DIR; make -j $EXEC_NAME)
#30 40 50 60 70 80 90 100 110 120
    for FLAGS_worker_threads in 10; do #30 60 90 100 120
    for FLAGS_dram_gib in 10; do
    for FLAGS_su_merge in false true; do
        (
        $EXEC_DIR/$EXEC_NAME \
            -worker_threads=$FLAGS_worker_threads \
            -dram_gib=$FLAGS_dram_gib \
            -csv_path=$CSV_PATH \
            -csv_truncate \
            -ssd_path="${SSD_PATH}" \
            -run_for_seconds=3 \
            -cool_pct=20 \
            -pp_threads=0 \
            -partition_bits=6 \
            -free_pct=1 \
            -print_fill_factors \
            -tag=urls \
            -in=/bulk/datasets/urls.vector \
            -su_merge=$FLAGS_su_merge
    )

    done
    done
    done
}

if [[ -n ${A} ]]; then
    benchmarkA
fi

exit 0
