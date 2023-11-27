#include "benchmark/filebench/webserver/config.h"

DEFINE_int32(webserver_no_files, 10000, "Number of HTML files");
DEFINE_uint32(webserver_file_read_per_txn, 10, "Number of HTML files to read per transaction");
DEFINE_uint32(webserver_log_entry_size, 16384, "Size of the log entry to append to the log file");
DEFINE_uint32(webserver_exec_seconds, 20, "Execution time");