#include "benchmark/wikipedia/config.h"

DEFINE_bool(wiki_index_evaluate_deduplication, false,
            "Whether to check for BLOB deduplication during prefix indexing, "
            "i.e. FLAGS_blob_indexing_variant == 2");
DEFINE_bool(wiki_clear_cache_before_expr, false,
            "Whether to clear the cache, i.e. page cache in OS or buffer pool in DB, "
            "before running the experiment");
DEFINE_string(wiki_workload_config_path, "/home/duynguyen/Workplace/blob/benchmark/src/wikipedia/summary.csv",
              "The CSV file which contains Wikipedia workload characteristics");
DEFINE_string(wiki_articles_path, "/home/duynguyen/Workplace/blob/benchmark/src/wikipedia/articles",
              "The file which contains all non-empty articles of enwiki in json format");
DEFINE_uint64(wiki_exec_seconds, 20, "Execution time");
