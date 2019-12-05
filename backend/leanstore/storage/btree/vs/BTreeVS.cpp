#include "BTreeVS.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore::buffermanager;
// -------------------------------------------------------------------------------------
namespace leanstore {
namespace btree {
namespace vs {
// -------------------------------------------------------------------------------------
BTree::BTree()
{
}
// -------------------------------------------------------------------------------------
void BTree::init(DTID dtid)
{
   this->dtid = dtid;
   auto root_write_guard = WritePageGuard<BTreeNode>::allocateNewPage(dtid);
   root_write_guard.init(true);
   root_swip = root_write_guard.bf;
}
// -------------------------------------------------------------------------------------
bool BTree::lookup(u8 *key, unsigned keyLength, u64 &payloadLength, u8 *result)
{
   u16 level = 0;
   while ( true ) {
      try {
         auto p_guard = ReadPageGuard<BTreeNode>::makeRootGuard(root_lock);
         ReadPageGuard c_guard(p_guard, root_swip);
         while ( !c_guard->isLeaf ) {
            Swip<BTreeNode> &c_swip = c_guard->lookupInner(key, keyLength);
            p_guard = std::move(c_guard);
            c_guard = ReadPageGuard(p_guard, c_swip);
            level++;
         }
         int pos = c_guard->lowerBound<true>(key, keyLength);
         if ( pos != -1 ) {
            payloadLength = c_guard->getPayloadLength(pos);
            memcpy(result, (c_guard->isLarge(pos)) ? c_guard->getPayloadLarge(pos) : c_guard->getPayload(pos), payloadLength);
            c_guard.recheck_done();
            return true;
         } else {
            c_guard.recheck_done();
            return false;
         }
      } catch ( RestartException e ) {
         restarts_counter++;
      }
   }
}
// -------------------------------------------------------------------------------------
void BTree::insert(u8 *key, unsigned keyLength, u64 payloadLength, u8 *payload)
{
   u32 mask = 1;
   u32 const max = 64; //MAX_BACKOFF
   static atomic<u64> last_bf;
   while ( true ) {
      try {
         auto p_guard = ReadPageGuard<BTreeNode>::makeRootGuard(root_lock);
         ReadPageGuard c_guard(p_guard, root_swip);
         while ( !c_guard->isLeaf ) {
            Swip<BTreeNode> &c_swip = c_guard->lookupInner(key, keyLength);
            p_guard = std::move(c_guard);
            c_guard = ReadPageGuard(p_guard, c_swip);
         }
         auto c_x_guard = WritePageGuard(std::move(c_guard));
         p_guard.kill();
         if ( c_x_guard->insert(key, keyLength, ValueType(reinterpret_cast<BufferFrame *>(payloadLength)), payload)) {
            last_bf = u64(c_x_guard.bf->header.pid);
            return;
         }
         // no more space, need to split
         auto p_x_guard = WritePageGuard(std::move(p_guard));
         splitNode(c_x_guard, p_x_guard, key, keyLength);
         continue;
      } catch ( RestartException e ) {
         for ( u32 i = mask; i; --i ) {
            _mm_pause();
         }
         mask = mask < max ? mask << 1 : max;
         restarts_counter++;
      }
   }
}
// -------------------------------------------------------------------------------------
void BTree::splitNode(WritePageGuard<BTreeNode> &node, WritePageGuard<BTreeNode> &parent, u8 *key, unsigned keyLength)
{
   // TODO: refactor
   if ( !parent.hasBf()) {
      // create new root
      auto new_root = WritePageGuard<BTreeNode>::allocateNewPage(dtid, false);
      auto new_left_node = WritePageGuard<BTreeNode>::allocateNewPage(dtid);
      new_root.keepAlive();
      new_left_node.init(node->isLeaf);
      new_root.init(false);
      // -------------------------------------------------------------------------------------
      new_root->upper = node.swip();
      root_swip.swizzle(new_root.bf);
      height++;
      // -------------------------------------------------------------------------------------
      BTreeNode::SeparatorInfo sepInfo = node->findSep();
      u8 sepKey[sepInfo.length];
      node->getSep(sepKey, sepInfo);
      // -------------------------------------------------------------------------------------
      // -------------------------------------------------------------------------------------
      node->split(new_root, new_left_node, sepInfo.slot, sepKey, sepInfo.length);
      return;
   }
   BTreeNode::SeparatorInfo sepInfo = node->findSep();
   unsigned spaceNeededParent = BTreeNode::spaceNeeded(sepInfo.length, parent->prefixLength);
   assert(!parent->isLeaf);
   if ( parent->requestSpaceFor(spaceNeededParent)) { // Is there enough space in the parent for the separator?
      u8 sepKey[sepInfo.length];
      node->getSep(sepKey, sepInfo);
      // -------------------------------------------------------------------------------------
      auto new_left_node = WritePageGuard<BTreeNode>::allocateNewPage(dtid);
      new_left_node.init(node->isLeaf);
      // -------------------------------------------------------------------------------------
      node->split(parent, new_left_node, sepInfo.slot, sepKey, sepInfo.length);
   } else {
      ensureSpace(parent, spaceNeededParent, key, keyLength); // Must split parent first to make space for separator
   }
}
// -------------------------------------------------------------------------------------
void BTree::ensureSpace(WritePageGuard<BTreeNode> &toSplit, unsigned spaceNeeded, u8 *key, unsigned keyLength)
{
   auto p_guard = ReadPageGuard<BTreeNode>::makeRootGuard(root_lock);
   if ( root_swip.bf == toSplit.bf ) {
      auto root_x_guard = WritePageGuard(std::move(p_guard));
      splitNode(toSplit, root_x_guard, key, keyLength);
      return;
   }
   ReadPageGuard c_guard(p_guard, root_swip);
   Swip<BTreeNode> *c_swip;

   auto search_condition = [&]() {
      c_swip = &c_guard->lookupInner(key, keyLength);
      return !(c_swip->bf == toSplit.bf);
   };
   while ( search_condition()) {
      p_guard = std::move(c_guard);
      c_guard = ReadPageGuard(p_guard, *c_swip);
   }
   auto to_split_parent_x_guard = WritePageGuard(std::move(c_guard));
   //Note: c_guard is already write locked by the caller
   splitNode(toSplit, to_split_parent_x_guard, key, keyLength);
}
// -------------------------------------------------------------------------------------
void BTree::update(u8 *key, unsigned keyLength, u64 payloadLength, u8 *payload)
{
   u32 mask = 1;
   u32 const max = 64; //MAX_BACKOFF
   while ( true ) {
      try {
         auto p_guard = ReadPageGuard<BTreeNode>::makeRootGuard(root_lock);
         ReadPageGuard c_guard(p_guard, root_swip);
         while ( !c_guard->isLeaf ) {
            Swip<BTreeNode> &c_swip = c_guard->lookupInner(key, keyLength);
            p_guard = std::move(c_guard);
            c_guard = ReadPageGuard(p_guard, c_swip);
         }
         auto c_x_guard = WritePageGuard(std::move(c_guard));
         p_guard.kill();
         if ( c_x_guard->update(key, keyLength, payloadLength, payload)) {
            return;
         }
         // no more space, need to split
         auto p_x_guard = WritePageGuard(std::move(p_guard));
         splitNode(c_x_guard, p_x_guard, key, keyLength);
         continue;
      } catch ( RestartException e ) {
         for ( u32 i = mask; i; --i ) {
            _mm_pause();
         }
         mask = mask < max ? mask << 1 : max;
         restarts_counter++;
      }
   }
}
// -------------------------------------------------------------------------------------
bool BTree::remove(u8 *key, unsigned keyLength)
{
   //TODO:
   // remember, we can not keep count = 0
   /*
    * Plan:
    * check the right (only one) node if it is under filled
    * if yes, then lock exclusively
    * if there was not, and after deletion we got an empty
    * */
   u32 mask = 1;
   u32 const max = 64; //MAX_BACKOFF
   while ( true ) {
      try {
         auto p_guard = ReadPageGuard<BTreeNode>::makeRootGuard(root_lock);
         ReadPageGuard c_guard(p_guard, root_swip);
         while ( !c_guard->isLeaf ) {
            Swip<BTreeNode> &c_swip = c_guard->lookupInner(key, keyLength);
            p_guard = std::move(c_guard);
            c_guard = ReadPageGuard(p_guard, c_swip);
         }
         if()
         auto c_x_guard = WritePageGuard(std::move(c_guard));
         p_guard.kill();
         if ( !c_x_guard->remove(key, keyLength)){
            return false;
         }
         // TODO: merge after delete
//         if ( c_x_guard->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize ) {
//            // find neighbor and merge
//            if ( node != root && (parent->count >= 2) && (pos + 1) < parent->count ) {
//               BTreeNode *right = parent->getValue(pos + 1);
//               if ( right->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize )
//                  return node->merge(pos, parent, right);
//            }
//         }
         return true;
      } catch ( RestartException e ) {
         for ( u32 i = mask; i; --i ) {
            _mm_pause();
         }
         mask = mask < max ? mask << 1 : max;
         restarts_counter++;
      }
   }
}
// -------------------------------------------------------------------------------------
BTree::~BTree()
{
}
// -------------------------------------------------------------------------------------
struct DTRegistry::DTMeta BTree::getMeta()
{
   DTRegistry::DTMeta btree_meta = {
           .iterate_children=iterateChildrenSwips, .find_parent = findParent
   };
   return btree_meta;
}
// -------------------------------------------------------------------------------------
struct ParentSwipHandler BTree::findParent(void *btree_object, struct BufferFrame &bf)
{
   // Pre: bf is write locked
   auto &c_node = *reinterpret_cast<BTreeNode *>(bf.page.dt);
   auto &btree = *reinterpret_cast<BTree *>(btree_object);
   Swip<BufferFrame> *c_swip = &btree.root_swip.cast<BufferFrame>();
   // -------------------------------------------------------------------------------------
   assert(c_node.count > 0);
   u16 key_length = c_node.getFullKeyLength(0);
   auto key = make_unique<u8[]>(key_length);
   c_node.copyFullKey(0, key.get(), key_length);
   // -------------------------------------------------------------------------------------
   auto p_guard = ReadPageGuard<BTreeNode>::makeRootGuard(btree.root_lock);
   // check if bf is the root node
   if ( c_swip->bf == &bf ) {
      p_guard.recheck_done();
      return {
              .swip = *c_swip, .guard = p_guard.bf_s_lock
      };
   }
   // -------------------------------------------------------------------------------------
   ReadPageGuard c_guard(p_guard, btree.root_swip);
   auto search_condition = [&]() {
      c_swip = &c_guard->lookupInner(key.get(), key_length).cast<BufferFrame>();
      return (c_swip->bf != &bf);
   };
   while ( search_condition()) {
      p_guard = std::move(c_guard);
      c_guard = ReadPageGuard(p_guard, c_swip->cast<BTreeNode>());
   }
   return {.swip = *c_swip, .guard = c_guard.bf_s_lock};
}
// -------------------------------------------------------------------------------------
void BTree::iterateChildrenSwips(void */*btree_object * / ,
   struct BufferFrame &bf, std::function<bool(Swip<BufferFrame> &)>
   callback)
   {
      // Pre: bf is read locked
      auto &c_node = *reinterpret_cast<BTreeNode *>(bf.page.dt);
      if ( c_node.isLeaf ) {
         return;
      }
      for ( u16 i = 0; i < c_node.count; i++ ) {
         if ( !callback(c_node.getValue(i).cast<BufferFrame>())) {
            return;
         }
      }
      callback(c_node.upper.cast<BufferFrame>());
   }
// Helpers
// -------------------------------------------------------------------------------------
   unsigned countInner(BTreeNode *node)
   {
//   if ( node->isLeaf )
//      return 0;
//   unsigned sum = 1;
//   for ( unsigned i = 0; i < node->count; i++ )
//      sum += countInner(node->getValue(i));
//   sum += countInner(node->upper);
//   return sum;
   }
// -------------------------------------------------------------------------------------
   unsigned countPages(BTreeNode *node)
   {
//   if ( node->isLeaf )
//      return 1;
//   unsigned sum = 1;
//   for ( unsigned i = 0; i < node->count; i++ )
//      sum += countPages(node->getValue(i));
//   sum += countPages(node->upper);
//   return sum;
   }
// -------------------------------------------------------------------------------------
   unsigned bytesFree(BTreeNode *node)
   {
//   if ( node->isLeaf )
//      return node->freeSpaceAfterCompaction();
//   unsigned sum = node->freeSpaceAfterCompaction();
//   for ( unsigned i = 0; i < node->count; i++ )
//      sum += bytesFree(node->getValue(i));
//   sum += bytesFree(node->upper);
//   return sum;
   }
// -------------------------------------------------------------------------------------
   unsigned height(BTreeNode *node)
   {
//   if ( node->isLeaf )
//      return 1;
//   return 1 + height(node->upper);
   }
// -------------------------------------------------------------------------------------
   void printInfos(BTreeNode *root, uint64_t totalSize)
   {
      uint64_t cnt = countPages(root);
      cout << "nodes:" << cnt << " innerNodes:" << countInner(root) << " space:" << (cnt * EFFECTIVE_PAGE_SIZE) / (float) totalSize << " height:" << height(root) << " rootCnt:" << root->count << " bytesFree:" << bytesFree(root) << endl;
   }
// -------------------------------------------------------------------------------------
}
}
}