#pragma once
#include "BTreeSlotted.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace btree
{
namespace nocc
{
enum class WAL_LOG_TYPE : u8 { WALInsert, WALUpdate, WALRemove, WALAfterBeforeImage, WALAfterImage, WALLogicalSplit, WALInitPage };
struct WALEntry {
   WAL_LOG_TYPE type;
};
struct WALBeforeAfterImage : WALEntry {
   u16 image_size;
   u8 payload[];
};
struct WALInitPage : WALEntry {
   DTID dt_id;
};
struct WALAfterImage : WALEntry {
   u16 image_size;
   u8 payload[];
};
struct WALLogicalSplit : WALEntry {
   PID parent_pid = -1;
   PID left_pid = -1;
   PID right_pid = -1;
   s32 right_pos = -1;
};
struct WALInsert : WALEntry {
   u16 key_length;
   u16 value_length;
   u8 payload[];
};
struct WALUpdate : WALEntry {
   u16 key_length;
   u8 payload[];
};
struct WALRemove : WALEntry {
   u16 key_length;
   u16 value_length;
   u8 payload[];
};
}  // namespace nocc
   // -------------------------------------------------------------------------------------
enum class OP_TYPE : u8 { POINT_READ, POINT_UPDATE, POINT_INSERT, POINT_DELETE, SCAN };
enum class OP_RESULT : u8 {
   OK = 0,
   NOT_FOUND = 1,
   DUPLICATE = 2,
   ABORT_TX = 3,
};
struct WALUpdateGenerator {
   void (*before)(u8* tuple, u8* entry);
   void (*after)(u8* tuple, u8* entry);
   u16 entry_size;
};
// -------------------------------------------------------------------------------------
struct BTree {
   // Interface
   // No side effects allowed!
   bool lookupOne(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback);
   // starts at the key >= start_key
   void scanAsc(u8* start_key, u16 key_length, function<bool(u8* key, u8* value, u16 value_length)>, function<void()>);
   // starts at the key + 1 and downwards
   void scanDesc(u8* start_key, u16 key_length, function<bool(u8* key, u16 key_length, u8* value, u16 value_length)>, function<void()>);
   void insert(u8* key, u16 key_length, u64 valueLength, u8* value);
   void updateSameSize(u8* key, u16 key_length, function<void(u8* value, u16 value_size)>, WALUpdateGenerator = {{}, {}, 0});
   bool remove(u8* key, u16 key_length);  // No side effects allowed!
   // -------------------------------------------------------------------------------------
#include "BTreeLL.hpp"
   // -------------------------------------------------------------------------------------
   // SI
#include "BTreeSI.hpp"
   // -------------------------------------------------------------------------------------
   // VI [WIP]
#include "BTreeVI.hpp"
   // -------------------------------------------------------------------------------------
   // VW [WIP]
#include "BTreeVW.hpp"
};
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
