#!/bin/bash -f

set -x
set -e

touch ssd
EXEC_DIR=./
EXEC_NAME="tpcc"
TAG=${TAG:-cloud}

function benchmarkA() {
    CSV_PATH="$(pwd)/"$TAG
    rm -f $CSV_PATH*.csv
    for FLAGS_worker_threads in 1 8 10 16 20 30 32 40 48 50 60 64 70 80 90 96 100 120 128; do
    for FLAGS_dram_gib in 80; do
    for FLAGS_tpcc_warehouse_count in 100; do #  60 100 120
    for FLAGS_pin_threads in false; do
    for FLAGS_cm_split in false true; do
    for FLAGS_mutex in true; do
        (
            $EXEC_DIR/tpcc \
            -tag=$TAG \
            -worker_threads=$FLAGS_worker_threads \
            -zipf_factor=0 \
            -dram_gib=$FLAGS_dram_gib \
            -csv_path=$CSV_PATH \
            -nocsv_truncate \
            -ssd_path=./ssd \
            -run_for_seconds=10 \
            -cool_pct=20 \
            -pp_threads=0 \
            -partition_bits=6 \
            -free_pct=1 \
            -tpcc_warehouse_count=$FLAGS_tpcc_warehouse_count \
            -pin_threads=$FLAGS_pin_threads \
            -cm_split=$FLAGS_cm_split \
            -mutex=$FLAGS_mutex \
            -nosu_merge \
            -print_tx_console
    )

    done
    done
    done
    done
    done
    done
}

benchmarkA
