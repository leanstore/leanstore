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
   BTreeNode::SeparatorInfo sepInfo = node->findSep();
   u8 sepKey[sepInfo.length];
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
      // -------------------------------------------------------------------------------------
      node->getSep(sepKey, sepInfo);
      // -------------------------------------------------------------------------------------
      node->split(new_root, new_left_node, sepInfo.slot, sepKey, sepInfo.length);
      // -------------------------------------------------------------------------------------
      height++;
      return;
   }
   unsigned spaceNeededParent = BTreeNode::spaceNeeded(sepInfo.length, parent->prefixLength);
   assert(!parent->isLeaf);
   if ( parent->requestSpaceFor(spaceNeededParent)) { // Is there enough space in the parent for the separator?
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
         int pos = -1;
         auto p_guard = ReadPageGuard<BTreeNode>::makeRootGuard(root_lock);
         ReadPageGuard c_guard(p_guard, root_swip);
         while ( !c_guard->isLeaf ) {
            pos = c_guard->lowerBound<false>(key, keyLength);
            Swip<BTreeNode> &c_swip = (pos == c_guard->count) ? c_guard->upper : c_guard->getValue(pos);
            p_guard = std::move(c_guard);
            c_guard = ReadPageGuard(p_guard, c_swip);
         }
         auto c_x_guard = WritePageGuard(std::move(c_guard));
         if ( !c_x_guard->remove(key, keyLength)) {
            return false;
         }
         if ( !p_guard.hasBf()) {
            // we are root, do nothing
            return true;
         }
         if ( c_x_guard->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize ) {
            auto p_x_guard = WritePageGuard(std::move(p_guard));
            tryMerge(c_x_guard, p_x_guard, pos, key, keyLength);
         }
         return true;
      } catch ( RestartException
                e ) {
         for ( u32 i = mask; i; --i ) {
            _mm_pause();
         }
         mask = mask < max ? mask << 1 : max;
         restarts_counter++;
      }
   }
}
// -------------------------------------------------------------------------------------
void BTree::tryMerge(WritePageGuard<BTreeNode> &node, WritePageGuard<BTreeNode> &parent, int pos, u8 *key, u32 keyLength)
{
   auto merge_left = [&]() {
      Swip<BTreeNode> &l_swip = parent->getValue(pos - 1);
      auto l_guard = ReadPageGuard(parent, l_swip);
      if ( l_guard->freeSpaceAfterCompaction() < BTreeNodeHeader::underFullSize ) {
         return false;
      }
      auto l_x_guard = WritePageGuard(std::move(l_guard));
      l_x_guard->merge(pos - 1, parent, node);
      l_x_guard.reclaim();
      return true;
   };
   auto merge_right = [&]() {
      Swip<BTreeNode> &r_swip = parent->getValue(pos + 1);
      auto r_guard = ReadPageGuard(parent, r_swip);
      if ( r_guard->freeSpaceAfterCompaction() < BTreeNodeHeader::underFullSize ) {
         return false;
      }
      auto r_x_guard = WritePageGuard(std::move(r_guard));
      node->merge(pos, parent, r_x_guard);
      node.reclaim();
      return true;
   };
   if ( parent->count >= 2 ) {
      bool merged = false;
      if ( pos + 1 < parent->count ) {
         if ( merge_right()) {
            merged = true;
         }
      }
      if ( !merged && pos > 0 ) {
         if ( merge_left()) {
         }
      }
      // -------------------------------------------------------------------------------------
      if ( parent->freeSpaceAfterCompaction() >= BTreeNode::underFullSize ) {
         if ( root_swip.bf != parent.bf )
            tryEnsureFillingGrade(parent, key, keyLength);
      }
   }
}
// -------------------------------------------------------------------------------------
void BTree::tryEnsureFillingGrade(WritePageGuard<BTreeNode> &toMerge, u8 *key, u32 keyLength)
{
   auto p_guard = ReadPageGuard<BTreeNode>::makeRootGuard(root_lock);
   if ( root_swip.bf == toMerge.bf ) {
      return;
   }
   ReadPageGuard c_guard(p_guard, root_swip);
   Swip<BTreeNode> *c_swip;
   int pos = -1;
   auto search_condition = [&]() {
      pos = c_guard->lowerBound<false>(key, keyLength);
      c_swip = &((pos == c_guard->count) ? c_guard->upper : c_guard->getValue(pos));
      return !(c_swip->bf == toMerge.bf);
   };
   while ( search_condition()) {
      p_guard = std::move(c_guard);
      c_guard = ReadPageGuard(p_guard, *c_swip);
   }
   auto to_merge_parent_x_guard = WritePageGuard(std::move(c_guard));
   assert(pos != -1);
   assert(c_swip->bf == toMerge.bf);
   //Note: c_guard is already write locked by the caller
   tryMerge(toMerge, to_merge_parent_x_guard, pos, key, keyLength);
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
void BTree::iterateChildrenSwips(void *, BufferFrame &bf, std::function<bool(Swip<BufferFrame> &)> callback)
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
unsigned BTree::countPages()
{
   auto p_guard = ReadPageGuard<BTreeNode>::makeRootGuard(root_lock);
   ReadPageGuard c_guard(p_guard, root_swip);
   return iterateAllPages(c_guard, [](BTreeNode &) {
      return 1;
   }, [](BTreeNode &) {
      return 1;
   });
}
// -------------------------------------------------------------------------------------
s64 BTree::iterateAllPages(ReadPageGuard<BTreeNode> &node, std::function<s64(BTreeNode &)> inner, std::function<s64(BTreeNode &)> leaf)
{
   if ( node->isLeaf ) {
      return leaf(node.ref());
   }
   s64 res = inner(node.ref());
   for ( u16 i = 0; i < node->count; i++ ) {
      Swip<BTreeNode> &c_swip = node->getValue(i);
      auto c_guard = ReadPageGuard(node, c_swip);
      res += iterateAllPages(c_guard, inner, leaf);
   }
   Swip<BTreeNode> &c_swip = node->upper;
   auto c_guard = ReadPageGuard(node, c_swip);
   res += iterateAllPages(c_guard, inner, leaf);
   return res;
}
// -------------------------------------------------------------------------------------
unsigned BTree::countInner()
{
   auto p_guard = ReadPageGuard<BTreeNode>::makeRootGuard(root_lock);
   ReadPageGuard c_guard(p_guard, root_swip);
   return iterateAllPages(c_guard, [](BTreeNode &) {
      return 1;
   }, [](BTreeNode &) {
      return 0;
   });
}
// -------------------------------------------------------------------------------------
unsigned BTree::bytesFree()
{
   auto p_guard = ReadPageGuard<BTreeNode>::makeRootGuard(root_lock);
   ReadPageGuard c_guard(p_guard, root_swip);
   return iterateAllPages(c_guard, [](BTreeNode &inner) {
      return inner.freeSpaceAfterCompaction();
   }, [](BTreeNode &leaf) {
      return leaf.freeSpaceAfterCompaction();
   });
}
// -------------------------------------------------------------------------------------
void BTree::printInfos(uint64_t totalSize)
{
   auto p_guard = ReadPageGuard<BTreeNode>::makeRootGuard(root_lock);
   ReadPageGuard r_guard(p_guard, root_swip);
   uint64_t cnt = countPages();
   cout << "nodes:" << cnt << " innerNodes:" << countInner() << " space:" << (cnt * EFFECTIVE_PAGE_SIZE) / (float) totalSize << " height:" << height << " rootCnt:" << r_guard->count << " bytesFree:" << bytesFree() << endl;
}
// -------------------------------------------------------------------------------------
}
}
}