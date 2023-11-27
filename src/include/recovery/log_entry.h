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
    BLOB_ENTRY      = 5,
    FREE_PAGE       = 6,
    REUSE_PAGE      = 7,
    PAGE_IMG        = 8,
  };

  u32 chksum = 99;
  Type type;
  logid_t lsn;
  u64 size;

  void ComputeChksum() { chksum = ComputeCRC(reinterpret_cast<u8 *>(this) + sizeof(chksum), size - sizeof(chksum)); }

  void ValidateChksum() const {
    Ensure(chksum == ComputeCRC(reinterpret_cast<const u8 *>(this) + sizeof(chksum), size - sizeof(chksum)));
  }
};

struct LogMetaEntry : LogEntry {};

struct DataEntry : LogEntry {
  pageid_t pid;      // the page id which contains the modification stored in this log
  logid_t gsn;       // the gsn of this log entry
  indexid_t idx_id;  // the index which owns this log record
};

struct BlobEntry : LogEntry {
  u32 part_id;  // Allow 2^32 parts of FLAGS_blob_log_segment_size
  u8 payload[];
};

struct FreePageEntry : LogEntry {
  pageid_t start_pid;  // the start pid of the large page to be freed
  pageid_t lp_size;    // number of pages to be freed, starting from `start_pid`
  logid_t gsn;         // the gsn of this log entry
};

struct PageImgEntry : LogEntry {
  pageid_t pid;  // the page id which contains the modification stored in this log
  logid_t gsn;   // the gsn of this log entry
  u8 payload[];
};

}  // namespace leanstore::recovery