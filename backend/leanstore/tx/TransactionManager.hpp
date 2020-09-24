#include "Exceptions.hpp"
#include "Units.hpp"
#include "leanstore/Config.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <mutex>
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace tx
{
// -------------------------------------------------------------------------------------
struct WALEntry {
  u64 LSN;
  u64 GSN;
};
// -------------------------------------------------------------------------------------
struct WALPartition {
  static constexpr u64 CHUNKS_COUNT = 5;
  static constexpr u64 START_CHUNK = 0;
  struct WALChunk {
    static constexpr u64 CHUNK_SIZE = 1024 * 1024 * 20;
    u64 free_space = CHUNK_SIZE;
    u8 data[CHUNK_SIZE];
  };
  WALChunk chunks[CHUNKS_COUNT];
  WALChunk* current_chunk = &chunks[START_CHUNK];
  // -------------------------------------------------------------------------------------
  u64 LSN_counter = 0;
  u64 max_GSN = 0;
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
  static unique_ptr<ThreadData[]> per_thread;

 public:
  static u64 threads_counter;
  static thread_local s64 thread_id;
  static std::mutex mutex;
  // -------------------------------------------------------------------------------------
  TXMG();
  // -------------------------------------------------------------------------------------
  static void registerThread();
  inline static ThreadData& my()
  {
    assert(thread_id != -1);
    return per_thread[thread_id];
  }
  // -------------------------------------------------------------------------------------
  static u8* reserveEntry(u64 requested_size);
  // -------------------------------------------------------------------------------------
  static void startTX();
  static void commitTX();
  static void abortTX();
};
// -------------------------------------------------------------------------------------
}  // namespace tx
}  // namespace leanstore
