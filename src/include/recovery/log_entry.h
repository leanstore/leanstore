#pragma once

#include "common/constants.h"
#include "common/exceptions.h"
#include "common/typedefs.h"
#include "common/utils.h"

#include <atomic>

namespace leanstore::recovery {

struct LogEntry {
  enum class Type : u8 {
    CARRIAGE_RETURN = 0,
    TX_START        = 1,
    TX_COMMIT       = 2,
    TX_ABORT        = 3,
    DATA_ENTRY      = 4,
    FREE_PAGE       = 6,
    REUSE_PAGE      = 7,
    FREE_EXTENT     = 8,
    REUSE_EXTENT    = 9,
    PAGE_IMG        = 10,
  };

  u32 chksum = 99;
  Type type;
  timestamp_t lsn;
  u64 size;

  void ComputeChksum() { chksum = ComputeCRC(reinterpret_cast<u8 *>(this) + sizeof(chksum), size - sizeof(chksum)); }

  void ValidateChksum() const {
    Ensure(chksum == ComputeCRC(reinterpret_cast<const u8 *>(this) + sizeof(chksum), size - sizeof(chksum)));
  }
};

struct LogMetaEntry : LogEntry {};

struct DataEntry : LogEntry {
  pageid_t pid;     // the page id which contains the modification stored in this log
  timestamp_t gsn;  // the gsn of this log entry
};

struct FreePageEntry : LogEntry {
  pageid_t pid;     // the pid of the large page to be freed
  timestamp_t gsn;  // the gsn of this log entry
};

struct FreeExtentEntry : LogEntry {
  pageid_t start_pid;  // the start pid of the large page to be freed
  pageid_t lp_size;    // number of pages to be freed, starting from `start_pid`
  timestamp_t gsn;     // the gsn of this log entry
};

struct PageImgEntry : LogEntry {
  pageid_t pid;     // the page id which contains the modification stored in this log
  timestamp_t gsn;  // the gsn of this log entry
  u8 payload[];
};

}  // namespace leanstore::recovery