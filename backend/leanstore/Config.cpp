#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
DEFINE_double(dram_gib, 1, "");
DEFINE_double(ssd_gib, 1700, "");
DEFINE_uint32(free_pct, 1, "pct");
DEFINE_uint32(partition_bits, 6, "bits per partition");
DEFINE_uint32(pp_threads, 1, "number of page provider threads");
DEFINE_bool(worker_page_eviction, false, "");
// -------------------------------------------------------------------------------------
DEFINE_string(csv_path, "./log", "");
DEFINE_bool(csv_truncate, false, "");
DEFINE_string(ssd_path, "./leanstore", "Position of SSD, gets persisted");
DEFINE_uint32(write_buffer_size, 1024, "");
DEFINE_bool(trunc, false, "Truncate file");
DEFINE_uint32(falloc, 0, "Preallocate GiB");
// -------------------------------------------------------------------------------------
DEFINE_bool(print_debug, true, "");
DEFINE_bool(print_tx_console, true, "");
DEFINE_uint32(print_debug_interval_s, 1, "");
DEFINE_bool(profiling, false, "");
DEFINE_bool(profile_latency, false, "");
DEFINE_bool(crc_check, false, "");
// -------------------------------------------------------------------------------------
DEFINE_uint32(worker_threads, 4, "");
DEFINE_bool(cpu_counters, true, "Disable if HW does not have enough counters for all threads");
DEFINE_bool(pin_threads, false, "Responsibility of the driver");
DEFINE_bool(smt, true, "Simultaneous multithreading");
// -------------------------------------------------------------------------------------
DEFINE_bool(root, false, "does this process have root rights ?");
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
DEFINE_string(zipf_path, "/bulk/zipf", "");
DEFINE_double(zipf_factor, 0.0, "");
DEFINE_double(target_gib, 0.0, "size of dataset in gib (exact interpretation depends on the driver)");
DEFINE_uint64(run_for_seconds, 10, "Keep the experiment running for x seconds");
DEFINE_uint64(warmup_for_seconds, 10, "Warmup for x seconds");
// -------------------------------------------------------------------------------------
DEFINE_bool(contention_split, true, "");
DEFINE_uint64(cm_update_on, 7, "as exponent of 2");
DEFINE_uint64(cm_period, 14, "as exponent of 2");
DEFINE_uint64(cm_slowpath_threshold, 1, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(xmerge, false, "");
DEFINE_uint64(xmerge_k, 5, "");
DEFINE_double(xmerge_target_pct, 80, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(optimistic_scan, true, "Jump to next leaf directly if the pointer in the parent has not changed");
DEFINE_bool(measure_time, false, "");
// -------------------------------------------------------------------------------------
DEFINE_double(tmp1, 0.0, "for ad-hoc experiments");
DEFINE_double(tmp2, 0.0, "");
DEFINE_double(tmp3, 0.0, "");
DEFINE_double(tmp4, 0.0, "");
DEFINE_double(tmp5, 0.0, "");
DEFINE_double(tmp6, 0.0, "");
DEFINE_double(tmp7, 0.0, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(btree_print_height, false, "Print BTree height in destructor");
DEFINE_bool(btree_print_tuples_count, false, "Print # tuples in each BTree in destructor");
DEFINE_bool(btree_prefix_compression, true, "");
DEFINE_bool(btree_heads, true, "Enable heads optimization in lowerBound search");
DEFINE_int64(btree_hints, 1, "0: disabled 1: serial 1: AVX512");
DEFINE_bool(nc_reallocation, false, "Reallocate hot pages in non-clustered btree index");
// -------------------------------------------------------------------------------------
DEFINE_bool(bulk_insert, false, "");
// -------------------------------------------------------------------------------------
DEFINE_int64(trace_dt_id, -1, "Print a stack trace for page reads for this DT ID");
DEFINE_int64(trace_trigger_probability, 100, "");
DEFINE_bool(pid_tracing, false, "");
// -------------------------------------------------------------------------------------
DEFINE_string(tag, "", "Unique identifier for this, will be appended to each line csv");
// -------------------------------------------------------------------------------------
DEFINE_bool(optimistic_parent_pointer, false, "");
DEFINE_bool(out_of_place, false, "Out of place writes");
DEFINE_uint64(replacement_chunk_size, 64, "Replacement strategy chunk size");
DEFINE_bool(recycle_pages, true, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(wal, true, "");
DEFINE_bool(wal_rfa, true, "Remote Flush Avoidance (RFA)");
DEFINE_bool(wal_tuple_rfa, true, "tuple-wise tracking");
DEFINE_uint64(wal_offset_gib, 10, "");
DEFINE_bool(wal_pwrite, false, "Does not really write logs on SSD");
DEFINE_bool(wal_fsync, false, "");
DEFINE_int64(wal_variant, 0, "");
DEFINE_uint64(wal_log_writers, 1, "");
DEFINE_uint64(wal_buffer_size, 1024 * 1024 * 10, "");
// -------------------------------------------------------------------------------------
DEFINE_string(bookkeeper_jar_directories,
              "bookkeeper-wal/target:bookkeeper-wal/target/maven-dependencies",
              "List of directories containing all neccessary classpaths separated by ':'");
DEFINE_string(bookkeeper_metadata_uri, "zk+hierarchical://localhost:2181/ledgers", "URI to BookKeeper's metadata service");
DEFINE_int32(bookkeeper_ensemble, 3, "BookKeeper ledger ensemble size");
DEFINE_int32(bookkeeper_quorum, 3, "BookKeeper ledger quorum size");
// -------------------------------------------------------------------------------------
DEFINE_string(isolation_level, "si", "options: ru (READ_UNCOMMITTED), rc (READ_COMMITTED), si (SNAPSHOT_ISOLATION), ser (SERIALIZABLE)");
DEFINE_bool(mv, true, "Multi-version");
DEFINE_uint64(si_refresh_rate, 0, "");
DEFINE_bool(todo, true, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(vi, true, "BTree with SI using in-place version");
DEFINE_bool(vi_delta, true, "");
DEFINE_bool(vi_utodo, true, "");
DEFINE_bool(vi_rtodo, true, "");
DEFINE_bool(vi_flookup, false, "");
DEFINE_bool(vi_fremove, false, "");
DEFINE_bool(vi_update_version_elision, false, "");
DEFINE_bool(vi_fupdate_chained, false, "");
DEFINE_bool(vi_fupdate_fat_tuple, false, "");
DEFINE_uint64(vi_pgc_batch_size, 2, "");
DEFINE_bool(vi_fat_tuple, false, "");
DEFINE_string(vi_fat_tuple_dts, "", "");
DEFINE_bool(vi_fat_tuple_decompose, true, "");
DEFINE_uint64(vi_fat_tuple_trigger, 0, "1: oldest_oltp, 1: probability");
DEFINE_bool(vi_fat_tuple_alternative, false, "hit the previous version at every update");
DEFINE_bool(vi_dangling_pointer, true, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(olap_mode, true, "Use OLAP mode for long running transactions");
DEFINE_bool(graveyard, true, "Use Graveyard Index");
// -------------------------------------------------------------------------------------
DEFINE_bool(pgc, true, "Precise garbage collection/recycling");
DEFINE_uint64(pgc_variant, 0, "0 naive, 1 bit faster, 2 ...");
DEFINE_double(garbage_in_page_pct, 15, "Threshold to trigger page-wise garbage collection (%)");
DEFINE_uint64(vi_max_chain_length, 1000, "");
DEFINE_uint64(todo_batch_size, 1024, "");
DEFINE_bool(history_tree_inserts, true, "");
// -------------------------------------------------------------------------------------
DEFINE_bool(persist, false, "");
DEFINE_bool(recover, false, "");
DEFINE_string(persist_file, "./leanstore.json", "Where should the persist config be saved to?");
DEFINE_string(recover_file, "./leanstore.json", "Where should the recover config be loaded from?");
