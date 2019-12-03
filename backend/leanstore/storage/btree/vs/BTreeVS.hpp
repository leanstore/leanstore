#include "BTreeSlotted.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/storage/buffer-manager/PageGuard.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace leanstore::buffermanager;
// -------------------------------------------------------------------------------------
namespace leanstore{
namespace btree {
namespace vs {
// -------------------------------------------------------------------------------------
struct BTree {
   DTID dtid;
   // -------------------------------------------------------------------------------------
   atomic<u16> height = 1; //debugging
   atomic<u64> restarts_counter = 0; //debugging
   OptimisticLock root_lock = 0;
   Swip<BTreeNode> root_swip;
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   BTree();
   void init(DTID dtid);
   bool lookup(u8 *key, unsigned keyLength, u64 &payloadLength, u8 *result);
   void splitNode(WritePageGuard<BTreeNode> &node, WritePageGuard<BTreeNode> &parent, u8 *key, unsigned keyLength);
   void ensureSpace(WritePageGuard<BTreeNode> &toSplit, unsigned spaceNeeded, u8 *key, unsigned keyLength);
   void insert(u8 *key, unsigned keyLength, u64 payloadLength, u8 *payload = nullptr);
   bool remove(u8 *key, unsigned keyLength);
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   static DTRegistry::DTMeta getMeta();
   static ParentSwipHandler findParent(void *btree_object, BufferFrame &bf);
   static void iterateChildSwips(void */*btree_object*/, BufferFrame &bf, std::function<bool(Swip<BufferFrame> &)> callback);
   // -------------------------------------------------------------------------------------
   ~BTree();
};
// -------------------------------------------------------------------------------------
unsigned countInner(BTreeNode *node);
unsigned countPages(BTreeNode *node);
unsigned bytesFree(BTreeNode *node);
unsigned height(BTreeNode *node);
void printInfos(BTreeNode *root, uint64_t totalSize);
}
}
}