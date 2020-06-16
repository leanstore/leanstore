#!/bin/bash -f

set -x
set -e

EXEC_DIR=../../../../release/frontend/
EXEC_NAME="merge"
#FLAGS_AGGRESIVE=${AG:-true}

function benchmarkA() { # URLs
    CSV_PATH="$(pwd)/em_a"
    rm -f $CSV_PATH
    (cd $EXEC_DIR; make -j $EXEC_NAME)
#30 40 50 60 70 80 90 100 110 120
    for FLAGS_worker_threads in 10; do #30 60 90 100 120
    for FLAGS_dram_gib in 10; do
    for FLAGS_aggresive in false true; do
        (
        $EXEC_DIR/$EXEC_NAME \
            -worker_threads=$FLAGS_worker_threads \
            -dram_gib=$FLAGS_dram_gib \
            -csv_path=$CSV_PATH \
            -csv_truncate \
            -ssd_path="${SSD_PATH}" \
            -run_for_seconds=10 \
            -cool_pct=20 \
            -y=5 \
            -aggresive=$FLAGS_aggresive \
            -su_kwaymerge=5 \
            -pp_threads=0 \
            -partition_bits=6 \
            -free_pct=1 \
            -print_fill_factors \
            -tag=$FLAGS_aggresive \
            -in=/bulk/datasets/urls.vector \
            -su_merge
    )

    done
    done
    done
}

function benchmarkSeq() { # URLs
    CSV_PATH="$(pwd)/em_seq"
    rm -f $CSV_PATH"*"
    (cd $EXEC_DIR; make -j $EXEC_NAME)
#30 40 50 60 70 80 90 100 110 120
    for FLAGS_worker_threads in 10; do #30 60 90 100 120
    for FLAGS_dram_gib in 50; do
    for FLAGS_aggresive in false true; do
        (
        $EXEC_DIR/$EXEC_NAME \
            -worker_threads=$FLAGS_worker_threads \
            -dram_gib=$FLAGS_dram_gib \
            -target_gib=20 \
            -csv_path=$CSV_PATH \
            -nocsv_truncate \
            -ssd_path="${SSD_PATH}" \
            -run_for_seconds=30 \
            -cool_pct=20 \
            -y=5 \
            -aggresive=$FLAGS_aggresive \
            -tag=$FLAGS_aggresive \
            -su_kwaymerge=5 \
            -pp_threads=0 \
            -free_pct=1 \
            -norandom_insert \
            -print_fill_factors \
            -su_merge
    )

    done
    done
    done
}

function benchmarkRnd() { # URLs
    CSV_PATH="$(pwd)/em_rnd"
    rm -f $CSV_PATH
    (cd $EXEC_DIR; make -j $EXEC_NAME)
    for FLAGS_worker_threads in 10; do
    for FLAGS_dram_gib in 50; do
    for FLAGS_aggresive in false true; do
        (
        $EXEC_DIR/$EXEC_NAME \
            -worker_threads=$FLAGS_worker_threads \
            -dram_gib=$FLAGS_dram_gib \
            -target_gib=20 \
            -csv_path=$CSV_PATH \
            -csv_truncate \
            -ssd_path="${SSD_PATH}" \
            -run_for_seconds=10 \
            -cool_pct=20 \
            -y=5 \
            -aggresive=$FLAGS_aggresive \
            -tag=$FLAGS_aggresive \
            -su_kwaymerge=5 \
            -pp_threads=0 \
            -free_pct=1 \
            -random_insert \
            -print_fill_factors \
            -su_merge
    )

    done
    done
    done
}

if [[ -n ${A} ]]; then
    benchmarkA
fi

if [[ -n ${SEQ} ]]; then
    benchmarkSeq
fi

if [[ -n ${RND} ]]; then
    benchmarkRnd
fi

exit 0
