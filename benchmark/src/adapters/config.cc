#include "benchmark/adapters/config.h"

DEFINE_bool(fs_enable_fsync, false, "Filesystem Adapter: Whether to enable FSync or not");
DEFINE_int32(fs_fsync_rate, 1000, "Filesystem Adapter: When fsync() is enabled, FSync() trigger rate");
