#!/bin/bash -f

set -x
set -e

EXEC_DIR=../../../../release/frontend/
EXEC_NAME="tpcc"

function benchmarkA() {
    CSV_PATH="$(pwd)/tpcc_a"
    rm -f $CSV_PATH
    (cd $EXEC_DIR; make -j $EXEC_NAME)
#30 40 50 60 70 80 90 100 110 120
    for FLAGS_worker_threads in 1 10 20 30 40 50 60 70 80 90 100 110 120; do #30 60 90 100 120
    for FLAGS_dram_gib in 70; do
    for FLAGS_tpcc_warehouse_count in 100; do #  60 100 120
    for FLAGS_cm_split in false true; do
        (
        $EXEC_DIR/tpcc \
            -worker_threads=$FLAGS_worker_threads \
            -zipf_factor=0 \
            -dram_gib=$FLAGS_dram_gib \
            -csv_path=$CSV_PATH \
            -nocsv_truncate \
            -ssd_path="${SSD_PATH}" \
            -run_for_seconds=10 \
            -cool_pct=20 \
            -pp_threads=0 \
            -partition_bits=6 \
            -free_pct=1 \
            -tpcc_warehouse_count=$FLAGS_tpcc_warehouse_count \
            -cm_split=$FLAGS_cm_split \
            -su_merge=$FLAGS_cm_split
    )

    done
    done
    done
    done
}

function benchmarkB() {
    CSV_PATH="$(pwd)/B.csv"
    #rm -f $CSV_PATH
    (cd $EXEC_DIR; make -j $EXEC_NAME)

    for FLAGS_worker_threads in 60 120; do
    for FLAGS_dram_gib in 40; do
    for FLAGS_tpcc_warehouse_count in 100; do #  60 100 120
    for FLAGS_zipf_factor in 0 0.5 0.6 0.7 0.8 0.99; do
    for FLAGS_cm_split in false true; do
        (
        $EXEC_DIR/tpcc \
            -worker_threads=$FLAGS_worker_threads \
            -dram_gib=$FLAGS_dram_gib \
            -csv_path=$CSV_PATH \
            -nocsv_truncate \
            -ssd_path="${SSD_PATH}" \
            -run_for_seconds=20 \
            -cool_pct=20 \
            -pp_threads=4 \
            -partition_bits=6 \
            -free_pct=1 \
            -tpcc_warehouse_count=$FLAGS_tpcc_warehouse_count \
            -cm_split=$FLAGS_cm_split \
            -su_merge=$FLAGS_cm_split \
            -zipf_factor=$FLAGS_zipf_factor
    )
    done
    done
    done
    done
    done
}

function benchmarkC() {
    CSV_PATH="$(pwd)/C_skx.csv"
    rm -f $CSV_PATH
    (cd $EXEC_DIR; make -j $EXEC_NAME)

    for FLAGS_worker_threads in 14; do
    for FLAGS_dram_gib in 5; do
    for FLAGS_tpcc_warehouse_count in 10; do #  60 100 120
    for FLAGS_cm_split in false; do
    for FLAGS_su_merge in false true; do
    for FLAGS_zipf_factor in 0; do
        (
        $EXEC_DIR/tpcc \
            -worker_threads=$FLAGS_worker_threads \
            -dram_gib=$FLAGS_dram_gib \
            -csv_path=$CSV_PATH \
            -nocsv_truncate \
            -ssd_path="${SSD_PATH}" \
            -run_for_seconds=40 \
            -cool_pct=20 \
            -pp_threads=4 \
            -partition_bits=6 \
            -free_pct=1 \
            -tpcc_warehouse_count=$FLAGS_tpcc_warehouse_count \
            -cm_split=$FLAGS_cm_split \
            -su_merge=$FLAGS_su_merge \
            -zipf_factor=$FLAGS_zipf_factor
    )

    done
    done
    done
    done
    done
    done
}

function benchmarkF() {
    CSV_PATH="$(pwd)/F_backoff.csv"
    #rm -f $CSV_PATH
    (cd $EXEC_DIR; make -j $EXEC_NAME)
    # 10 15 20 30 32 34 36 38 40 44 48 56 60 70 80 90 100 110 115 120 124
    for FLAGS_worker_threads in 90; do
    for FLAGS_dram_gib in 120; do
    for FLAGS_x in 0 64 512 1024; do
    for FLAGS_tpcc_warehouse_affinity in false true; do
    for FLAGS_cm_split in false; do
    for FLAGS_tpcc_pin in false; do
        (
        $EXEC_DIR/tpcc \
            -worker_threads=$FLAGS_worker_threads \
            -dram_gib=$FLAGS_dram_gib \
            -csv_path=$CSV_PATH \
            -nocsv_truncate \
            -ssd_path="${SSD_PATH}" \
            -run_for_seconds=10 \
            -cool_pct=20 \
            -pp_threads=1 \
            -partition_bits=6 \
            -free_pct=1 \
            -x=$FLAGS_x \
            -tpcc_warehouse_count=$FLAGS_worker_threads \
            -tpcc_warehouse_affinity=$FLAGS_tpcc_warehouse_affinity \
            -tpcc_pin=$FLAGS_tpcc_pin \
            -cm_split=$FLAGS_cm_split \
            -su_merge=$FLAGS_cm_split
    )

    done
    done
    done
    done
    done
    done
}

if [[ -n ${A} ]]; then
    benchmarkA
fi

if [[ -n ${B} ]]; then
    benchmarkB
fi

if [[ -n ${C} ]]; then
    benchmarkC
fi

if [[ -n ${F} ]]; then
    benchmarkF
fi

exit 0
