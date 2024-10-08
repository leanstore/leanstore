#pragma once

#include "common/constants.h"
#include "common/typedefs.h"
#include "leanstore/config.h"
#include "recovery/log_entry.h"

namespace leanstore::storage {

enum class WalType : u8 {
  INSERT        = 0,
  AFTER_IMAGE   = 1,
  DELTA_IMAGE   = 2,
  REMOVE        = 3,
  LOGICAL_SPLIT = 4,
  MERGE_NODES   = 5,
  INIT_PAGE     = 6,
  NEW_ROOT      = 7
};

struct WALEntry : recovery::DataEntry {
  WalType type;
};

struct WALInitPage : WALEntry {};

struct WALNewRoot : WALEntry {};

struct WALLogicalSplit : WALEntry {
  pageid_t parent_pid = -1;
  pageid_t left_pid   = -1;
  pageid_t right_pid  = -1;
  u16 sep_slot        = -1;
};

struct WALMergeNodes : WALEntry {
  pageid_t parent_pid = -1;
  pageid_t left_pid   = -1;
  pageid_t right_pid  = -1;
  u16 left_pos        = -1;
};

struct WALAfterImage : WALEntry {
  u16 key_length;
  u16 value_length;
  u8 payload[];
};

struct WALInsert : WALEntry {
  u16 key_length;
  u16 value_length;
  u8 payload[];
};

struct WALRemove : WALEntry {
  u16 key_length;
  u16 value_length;
  u8 payload[];
};

struct WALDeltaImage : WALEntry {
  u16 key_length;
  u16 value_length;
  u8 payload[];
};

// --------------------------------------------------------------------------
// WAL macros to shorten WAL impl

#define WAL_RECORD(node_guard, wal_type, key, w_payload)                                                \
  {                                                                                                     \
    auto &entry = (node_guard).PrepareWalEntry<wal_type>((key).size() + (w_payload).size());            \
    std::tie(entry.key_length, entry.value_length) = std::make_tuple((key).size(), (w_payload).size()); \
    std::memcpy(entry.payload, (key).data(), (key).size());                                             \
    std::memcpy(entry.payload + (key).size(), (w_payload).data(), (w_payload).size());                  \
    (node_guard).SubmitActiveWalEntry();                                                                \
  }

}  // namespace leanstore::storage