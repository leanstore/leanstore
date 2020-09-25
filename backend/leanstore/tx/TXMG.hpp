#include "Exceptions.hpp"
#include "Units.hpp"
#include "leanstore/Config.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <mutex>
#include <set>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace txmg
{
// -------------------------------------------------------------------------------------
struct WALEntry {
  enum class Type : u8 { TX_START, DT_CHANGE, DT_SPECIFIC, TX_COMMIT, TX_ABORT };
  u16 size;
  Type type;
  LID lsn;
  u8 payload[];
};
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
struct WALPartition {
  static constexpr u64 CHUNKS_COUNT = 1;  // 5 Chunks per WAL, TODO: ring buffer, WAL Writer
  static constexpr u64 START_CHUNK = 0;
  struct WALChunk {
    static constexpr u64 CHUNK_SIZE = 1024 * 1024 * 1024;  // 20 MiB
    u8* write_ptr = data;
    const u8* end = data + CHUNK_SIZE;
    u8 data[CHUNK_SIZE];
    WALEntry& entry(u16 size)
    {
      WALEntry& entry = *reinterpret_cast<WALEntry*>(write_ptr);
      entry.size = sizeof(WALEntry) + size;
      write_ptr += entry.size;
      ensure(write_ptr < end);
      return entry;
    }
  };
  WALChunk chunks[CHUNKS_COUNT];
  WALChunk* current_chunk = &chunks[START_CHUNK];
  // -------------------------------------------------------------------------------------
  LID lsn_counter = 0;
  LID max_gsn = 0;
  // -------------------------------------------------------------------------------------
  // TODO: circular buffer
  std::mutex mutex;  // sync with WAL Writer
  u64 max_flushed_GSN = 0;
  s16 free_chunk = CHUNKS_COUNT - 1, write_chunk = START_CHUNK;
};
// -------------------------------------------------------------------------------------
struct Transaction {
  enum class STATE { IDLE, STARTED, COMMITED, ABORTED };
  STATE state = STATE::IDLE;
  u64 start_gsn, current_gsn;
  DTID current_dt_id = -1;
};
// -------------------------------------------------------------------------------------
struct ThreadData {
  Transaction tx;
  WALPartition wal;
};
// -------------------------------------------------------------------------------------
// MUST be allocated dynamically
class TXMG
{
 private:
  static std::set<ThreadData*> all_threads;
  static thread_local ThreadData* thread_data;

 public:
  static std::mutex mutex;
  // -------------------------------------------------------------------------------------
  TXMG();
  ~TXMG();
  // -------------------------------------------------------------------------------------
  static void registerThread();
  static void removeThread();
  inline static ThreadData& my()
  {
    if (thread_data == nullptr)
      registerThread();
    assert(thread_data != nullptr);
    return *thread_data;
  }
  // -------------------------------------------------------------------------------------
  static u8* reserveEntry(DTID dt_id, u64 requested_size);
  // -------------------------------------------------------------------------------------
  static void startTX();
  static void commitTX();
  static void abortTX();
};
// -------------------------------------------------------------------------------------
}  // namespace txmg
}  // namespace leanstore
