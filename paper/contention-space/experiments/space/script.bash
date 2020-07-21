#!/bin/bash -f
# Runs on Skylake
set -x
set -e

BUILD=${BUILD:-release}
EXEC_DIR=../../../../$BUILD/frontend/
EXEC_NAME="merge"
IN=${IN:-/bulk/datasets/urls.vector}
FLAGS_aggressive=${FLAGS_aggresive:-true}
FLAGS_print_fill_factors=${FLAGS_print_fill_factors:-true}


function benchmarkA() { # URLs
    CSV_PATH=${CSV_PATH:-"$(pwd)/em_a"}
    rm -f ${CSV_PATH}*.csv
    (cd $EXEC_DIR; make -j $EXEC_NAME)
    for FLAGS_worker_threads in 10; do
    for FLAGS_target_gib in 20; do
    for FLAGS_dataset in strings integers; do
    for FLAGS_insertion_order in seq rnd; do #  integers rnd
    for FLAGS_su_kwaymerge in 3 4 5 6 7 8 9 10 11 12 13 14 15; do
        FLAGS_dram_gib=$[FLAGS_target_gib*25/10]
        if [ "$FLAGS_dataset" = "strings" ]; then #
            FLAGS_dram_gib=9
            if [ "$FLAGS_insertion_order" = "seq" ]; then #
                FLAGS_in=$IN".sorted"
            else
                FLAGS_in=$IN".random"
            fi
        fi

        (
        $EXEC_DIR/$EXEC_NAME \
            -worker_threads=$FLAGS_worker_threads \
            -dram_gib=$FLAGS_dram_gib \
            -target_gib=$FLAGS_target_gib \
            -csv_path=$CSV_PATH \
            -nocsv_truncate \
            -ssd_path="${SSD_PATH}" \
            -run_for_seconds=30 \
            -cool_pct=20 \
            -y=5 \
            -pp_threads=0 \
            -partition_bits=6 \
            -free_pct=1 \
            -print_fill_factors=$FLAGS_print_fill_factors \
            -aggressive\
            -tag=$FLAGS_dataset"-"$FLAGS_insertion_order \
            -insertion_order=$FLAGS_insertion_order \
            -dataset=$FLAGS_dataset \
            -in=$FLAGS_in \
            -su_merge \
            -su_kwaymerge=$FLAGS_su_kwaymerge
    )

    done
    done
    done
    done
    done
}


if [[ -n ${A} ]]; then
    benchmarkA
    exit 0
fi

function benchmarkS() { # Sizes
    CSV_PATH=${CSV_PATH:-"$(pwd)/size"}
    rm -f ${CSV_PATH}*.csv
    (cd $EXEC_DIR; make -j $EXEC_NAME)
    for FLAGS_worker_threads in 10; do
    for FLAGS_dram_gib in 20; do
    for FLAGS_dataset in urls emails wikititles2; do
    for FLAGS_insertion_order in seq rnd; do
    for FLAGS_su_kwaymerge in 5; do
      if [ "$FLAGS_insertion_order" = "seq" ]; then #
        FLAGS_in=$IN$FLAGS_dataset".vector.sorted"
      else
         FLAGS_in=$IN$FLAGS_dataset".vector.random"
      fi

        (
        $EXEC_DIR/$EXEC_NAME \
            -worker_threads=$FLAGS_worker_threads \
            -dram_gib=$FLAGS_dram_gib \
            -csv_path=$CSV_PATH \
            -nocsv_truncate \
            -ssd_path="${SSD_PATH}" \
            -run_for_seconds=30 \
            -cool_pct=20 \
            -pp_threads=0 \
            -partition_bits=6 \
            -free_pct=1 \
            -print_fill_factors=false \
            -aggressive\
            -tag=$FLAGS_dataset"-"$FLAGS_insertion_order \
            -insertion_order=$FLAGS_insertion_order \
            -dataset=strings \
            -in=$FLAGS_in \
            -su_merge \
            -su_kwaymerge=$FLAGS_su_kwaymerge
    )

    done
    done
    done
    done
    done
}


if [[ -n ${S} ]]; then
    benchmarkS
    exit 0
fi



function benchmarkB() {
    CSV_PATH=${CSV_PATH:-"$(pwd)/integers"}
    rm -f ${CSV_PATH}*.csv
    (cd $EXEC_DIR; make -j $EXEC_NAME)
    for FLAGS_worker_threads in 1; do
    for FLAGS_target_gib in 20; do
    for FLAGS_dataset in integers; do
    for FLAGS_insertion_order in rnd; do #  integers rnd
    for FLAGS_su_target_pct in 90; do
    for FLAGS_su_kwaymerge in 5; do
    for FLAGS_bstar in false true; do
        FLAGS_dram_gib=$[FLAGS_target_gib*25/10]
        (
        $EXEC_DIR/$EXEC_NAME \
            -worker_threads=$FLAGS_worker_threads \
            -dram_gib=$FLAGS_dram_gib \
            -target_gib=$FLAGS_target_gib \
            -csv_path=$CSV_PATH \
            -nocsv_truncate \
            -ssd_path="${SSD_PATH}" \
            -run_for_seconds=10 \
            -cool_pct=20 \
            -pp_threads=0 \
            -partition_bits=6 \
            -free_pct=1 \
            -noprint_fill_factors \
            -noaggressive\
            -tag=$FLAGS_dataset"-"$FLAGS_insertion_order \
            -insertion_order=$FLAGS_insertion_order \
            -dataset=$FLAGS_dataset \
            -in=$FLAGS_in \
            -bstar=$FLAGS_bstar \
            -su_merge \
            -su_target_pct=$FLAGS_su_target_pct \
            -su_kwaymerge=$FLAGS_su_kwaymerge
    )

    done
    done
    done
    done
    done
    done
    done
}

if [[ -n ${B} ]]; then
    benchmarkB
    exit 0
fi
