#!/bin/bash -f

set -x
set -e

BUILD=${BUILD:-release}
EXEC_DIR=../../../../$BUILD/frontend/
EXEC_NAME="merge"
IN=${FLAGS_in:-/bulk/datasets/urls.vector}
FLAGS_aggressive=${FLAGS_aggresive:-true}
FLAGS_print_fill_factors=${FLAGS_print_fill_factors:-true}
CSV_PATH=${CSV_PATH:-"$(pwd)/em_a"}


function benchmarkA() { # URLs
    rm -f $CSV_PATH"*.csv"
    (cd $EXEC_DIR; make -j $EXEC_NAME)
    for FLAGS_worker_threads in 10; do
    for FLAGS_target_gib in 2; do
    for FLAGS_dataset in strings integers; do
    for FLAGS_insertion_order in seq rnd; do #  integers rnd
    for FLAGS_su_kwaymerge in 3 4 5 6 7 8 9 10 11 12 13 14 15; do
        FLAGS_dram_gib=$[FLAGS_target_gib*25/10]
        if [ "$FLAGS_dataset" = "strings" ]; then #
            FLAGS_dram_gib=9
            if [ "$FLAGS_insertion_order" = "seq" ]; then #
                FLAGS_in=$IN".sorted"
            else
                FLAGS_in=$IN
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
fi

exit 0
