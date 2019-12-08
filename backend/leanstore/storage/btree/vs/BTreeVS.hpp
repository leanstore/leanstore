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
   atomic<u16> height = 1; //debugging
   atomic<u64> restarts_counter = 0; //debugging
   atomic<u64> removed_bfs = 0; //debugging
   OptimisticLock root_lock = 0;
   Swip<BTreeNode> root_swip;
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   BTree();
   void init(DTID dtid);
   bool lookup(u8 *key, unsigned key_length, u64 &payload_length, u8 *result);
   // -------------------------------------------------------------------------------------
   void insert(u8 *key, unsigned key_length, u64 payloadLength, u8 *payload);
   void trySplit(BufferFrame &to_split);
   // -------------------------------------------------------------------------------------
   // TODO
   void update(u8 *key, unsigned key_length, u64 payloadLength, u8 *payload);
   // -------------------------------------------------------------------------------------
   bool remove(u8 *key, unsigned key_length);
   void tryMerge(WritePageGuard<BTreeNode> &node, WritePageGuard<BTreeNode> &parent, int pos, u8 *key, u32 key_length);
   void tryEnsureFillingGrade(WritePageGuard<BTreeNode> &toMerge, u8 *key, u32 key_length);
   // -------------------------------------------------------------------------------------
   static DTRegistry::DTMeta getMeta();
   static ParentSwipHandler findParent(void *btree_object, BufferFrame &to_find);
   static void iterateChildrenSwips(void */*btree_object*/, BufferFrame &bf, std::function<bool(Swip<BufferFrame> &)> callback);
   // -------------------------------------------------------------------------------------
   ~BTree();
   // -------------------------------------------------------------------------------------
   // Helpers
   s64 iterateAllPages(ReadPageGuard<BTreeNode> &node_guard, std::function<s64(BTreeNode &)> inner, std::function<s64(BTreeNode &)> leaf);
   unsigned countInner();
   unsigned countPages();
   unsigned bytesFree();
   void printInfos(uint64_t totalSize);
};
// -------------------------------------------------------------------------------------
}
}
}