#pragma once
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
DECLARE_double(dram_gib);
DECLARE_double(ssd_gib);
DECLARE_string(ssd_path);
DECLARE_uint32(worker_threads);
DECLARE_bool(cpu_counters);
DECLARE_bool(pin_threads);
DECLARE_bool(smt);
DECLARE_string(csv_path);
DECLARE_bool(csv_truncate);
DECLARE_uint32(cool_pct);
DECLARE_uint32(free_pct);
DECLARE_uint32(partition_bits);
DECLARE_uint32(write_buffer_size);
DECLARE_uint32(falloc);
DECLARE_uint32(pp_threads);
DECLARE_bool(trunc);
DECLARE_bool(root);
DECLARE_bool(print_debug);
DECLARE_bool(print_tx_console);
DECLARE_bool(profiling);
DECLARE_uint32(print_debug_interval_s);
// -------------------------------------------------------------------------------------
DECLARE_bool(contention_split);
DECLARE_uint64(cm_update_on);
DECLARE_uint64(cm_period);
DECLARE_uint64(cm_slowpath_threshold);
// -------------------------------------------------------------------------------------
DECLARE_bool(xmerge);
DECLARE_uint64(xmerge_k);
DECLARE_double(xmerge_target_pct);
// -------------------------------------------------------------------------------------
DECLARE_bool(optimistic_scan);
// -------------------------------------------------------------------------------------
DECLARE_string(zipf_path);
DECLARE_double(zipf_factor);
DECLARE_double(target_gib);
DECLARE_uint64(run_for_seconds);
DECLARE_uint64(warmup_for_seconds);
// -------------------------------------------------------------------------------------
DECLARE_uint64(backoff_strategy);
// -------------------------------------------------------------------------------------
DECLARE_uint64(backoff);
// -------------------------------------------------------------------------------------
DECLARE_double(tmp1);
DECLARE_double(tmp2);
DECLARE_double(tmp3);
DECLARE_double(tmp4);
DECLARE_double(tmp5);
DECLARE_double(tmp6);
DECLARE_double(tmp7);
// -------------------------------------------------------------------------------------
DECLARE_bool(btree_print_height);
DECLARE_bool(btree_print_tuples_count);
DECLARE_bool(nc_reallocation);
DECLARE_bool(bulk_insert);
// -------------------------------------------------------------------------------------
DECLARE_int64(trace_dt_id);
DECLARE_int64(trace_trigger_probability);
DECLARE_string(tag);
// -------------------------------------------------------------------------------------
DECLARE_bool(optimistic_parent_pointer);
DECLARE_bool(out_of_place);
// -------------------------------------------------------------------------------------
DECLARE_bool(wal);
DECLARE_bool(wal_rfa);
DECLARE_bool(wal_rfa_pmem_simulate);
DECLARE_uint64(wal_offset_gib);
DECLARE_bool(wal_io_hack);
DECLARE_bool(wal_fsync);
// -------------------------------------------------------------------------------------
DECLARE_string(isolation_level);
DECLARE_bool(mv);
DECLARE_bool(2pl);
DECLARE_uint64(si_refresh_rate);
DECLARE_bool(vw);
DECLARE_bool(todo);
// -------------------------------------------------------------------------------------
DECLARE_bool(vi);
DECLARE_bool(vi_delta);
DECLARE_bool(vi_utodo);
DECLARE_bool(vi_rtodo);
DECLARE_bool(vi_flookup);
DECLARE_bool(vi_fremove);
DECLARE_bool(vi_update_version_elision);
DECLARE_bool(vi_fupdate_chained);
DECLARE_bool(vi_fupdate_fat_tuple);
DECLARE_uint64(vi_fat_tuple_threshold);
DECLARE_uint64(vi_pgc_batch_size);
DECLARE_bool(vi_fat_tuple);
DECLARE_bool(vi_dangling_pointer);
DECLARE_bool(vi_fat_tuple_decompose);
// -------------------------------------------------------------------------------------
DECLARE_bool(olap_mode);
// -------------------------------------------------------------------------------------
DECLARE_bool(pgc);
DECLARE_uint64(pgc_variant);
DECLARE_double(garbage_in_page_pct);
DECLARE_uint64(vi_max_chain_length);
DECLARE_uint64(todo_batch_size);
DECLARE_bool(history_tree_inserts);
// -------------------------------------------------------------------------------------
DECLARE_bool(persist);
DECLARE_bool(recover);
DECLARE_string(persist_file);
DECLARE_string(recover_file);
