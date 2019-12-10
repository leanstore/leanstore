#include "BTreeSlotted.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/storage/buffer-manager/PageGuard.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace leanstore::buffermanager;
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace btree {
namespace vs {
// -------------------------------------------------------------------------------------
struct BTree {
   DTID dtid;
   // -------------------------------------------------------------------------------------
   atomic<u16> height = 1; // debugging
   atomic<u64> restarts_counter = 0; // debugging
   OptimisticLock root_lock = 0;
   Swip<BTreeNode> root_swip;
   // -------------------------------------------------------------------------------------
   BTree();
   // -------------------------------------------------------------------------------------
   void init(DTID dtid);
   ReadPageGuard<BTreeNode> findLeafForRead(u8 *key, u16 key_length);
   // No side effects allowed!
   bool lookup(u8 *key, u16 key_length, function<void(const u8 *, u16)> payload_callback);
   // -------------------------------------------------------------------------------------
   void scan(u8 *start_key, u16 key_length, function<bool(u8 *payload, u16 payload_length, function<string()> &)>, function<void()>);
   // -------------------------------------------------------------------------------------
   void insert(u8 *key, u16 key_length, u64 payloadLength, u8 *payload);
   void trySplit(BufferFrame &to_split);
   // -------------------------------------------------------------------------------------
   void updateSameSize(u8 *key, u16 key_length, function<void(u8 *payload, u16 payload_size)>);
   void update(u8 *key, u16 key_length, u64 payloadLength, u8 *payload);
   // -------------------------------------------------------------------------------------
   bool remove(u8 *key, u16 key_length);
   void tryMerge(BufferFrame &to_split);
   // -------------------------------------------------------------------------------------
   static DTRegistry::DTMeta getMeta();
   static ParentSwipHandler findParent(void *btree_object, BufferFrame &to_find);
   static void iterateChildrenSwips(void */*btree_object*/, BufferFrame &bf, std::function<bool(Swip<BufferFrame> &)> callback);
   // -------------------------------------------------------------------------------------
   ~BTree();
   // -------------------------------------------------------------------------------------
   // Helpers
   s64 iterateAllPages(std::function<s64(BTreeNode &)> inner, std::function<s64(BTreeNode &)> leaf);
   s64 iterateAllPagesRec(ReadPageGuard<BTreeNode> &node_guard, std::function<s64(BTreeNode &)> inner, std::function<s64(BTreeNode &)> leaf);
   unsigned countInner();
   u32 countPages();
   u32 countEntries();
   u32 bytesFree();
   void printInfos(uint64_t totalSize);
};
// -------------------------------------------------------------------------------------
}
}
}