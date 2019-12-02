#include "BTreeVS.hpp"
#include "leanstore/storage/buffer-manager/PageGuard.hpp"
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
   auto root_write_guard = WritePageGuard<BTreeNode>::allocateNewPage(dtid);
   root_write_guard.init(true);
   root_swip = root_write_guard.bf;
}
// -------------------------------------------------------------------------------------
bool BTree::lookup(u8 *key, unsigned keyLength, u64 &payloadLength, u8 *result)
{
   while ( true ) {
      try {
         auto p_guard = ReadPageGuard<BTreeNode>::makeRootGuard(root_lock);
         ReadPageGuard c_guard(p_guard, root_swip);
         while ( !c_guard->isLeaf ) {
            Swip<BTreeNode> &c_swip = c_guard->lookupInner(key, keyLength);
            p_guard = std::move(c_guard);
            c_guard = ReadPageGuard(p_guard, c_swip);
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
      }
   }
}
// -------------------------------------------------------------------------------------
void BTree::insert(u8 *key, unsigned keyLength, u64 payloadLength, u8 *payload)
{
   //TODO
   while ( true ) {
      try {
         auto p_guard = ReadPageGuard<BTreeNode>::makeRootGuard(root_lock);
         ReadPageGuard c_guard(p_guard, root_swip);
         while ( !c_guard->isLeaf) {
            Swip<BTreeNode> &c_swip = c_guard->lookupInner(key, keyLength);
            p_guard = std::move(c_guard);
            c_guard = ReadPageGuard(p_guard, c_swip);
         }
         auto c_x_guard = WritePageGuard(std::move(c_guard));
         if ( c_x_guard->insert(key, keyLength, ValueType(reinterpret_cast<BufferFrame *>(payloadLength)), payload))
            return;
         // no more space, need to split
         auto p_x_guard = WritePageGuard(std::move(p_guard));
         splitNode(&c_x_guard.ref(), &p_x_guard.ref(), key, keyLength);
         continue;
      } catch ( RestartException e ) {
      }
   }
}
// -------------------------------------------------------------------------------------
void BTree::splitNode(BTreeNode *node, BTreeNode *parent, u8 *key, unsigned keyLength)
{
   if ( !parent ) {
      // create new root
      parent = new BTreeNode(false);
      parent->upper = node;
      root = parent;
   }
   BTreeNode::SeparatorInfo sepInfo = node->findSep();
   unsigned spaceNeededParent = BTreeNode::spaceNeeded(sepInfo.length, parent->prefixLength);
   assert(!parent->isLeaf);
   if ( parent->requestSpaceFor(spaceNeededParent)) { // Is there enough space in the parent for the separator?
      u8 sepKey[sepInfo.length];
      node->getSep(sepKey, sepInfo);
      node->split(parent, sepInfo.slot, sepKey, sepInfo.length);
   } else
      ensureSpace(parent, spaceNeededParent, key, keyLength); // Must split parent first to make space for separator
}
// -------------------------------------------------------------------------------------
void BTree::ensureSpace(BTreeNode *toSplit, unsigned spaceNeeded, u8 *key, unsigned keyLength)
{
   BTreeNode *node = root;
   BTreeNode *parent = nullptr;
   while ( node->isInner() && (node != toSplit)) {
      parent = node;
      node = node->lookupInner(key, keyLength);
   }
   splitNode(toSplit, parent, key, keyLength);
}
// -------------------------------------------------------------------------------------
bool BTree::remove(u8 *key, unsigned keyLength)
{
   // TODO: in next stages, we would sync the merge
   ensure(false);
   BTreeNode *node = root;
   BTreeNode *parent = nullptr;
   int pos = 0;
   while ( node->isInner()) {
      parent = node;
      pos = node->lowerBound<false>(key, keyLength);
      node = (pos == node->count) ? node->upper : node->getValue(pos);
   }
   static_cast<void>(parent);
   if ( !node->remove(key, keyLength))
      return false; // key not found
   if ( node->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize ) {
      // find neighbor and merge
      if ( node != root && (parent->count >= 2) && (pos + 1) < parent->count ) {
         BTreeNode *right = parent->getValue(pos + 1);
         if ( right->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize )
            return node->merge(pos, parent, right);
      }
   }
   return true;
}
// -------------------------------------------------------------------------------------
BTree::~BTree()
{
   root->destroy();
}
// -------------------------------------------------------------------------------------
struct DTRegistry::DTMeta BTree::getMeta()
{
   DTRegistry::DTMeta btree_meta = {
           .iterate_childern=iterateChildSwips, .find_parent = findParent
   };
   return btree_meta;
}
// -------------------------------------------------------------------------------------
struct ParentSwipHandler BTree::findParent(void *btree_object, struct BufferFrame &bf)
{
   // TODO
}
// -------------------------------------------------------------------------------------
void BTree::iterateChildSwips(void *, struct BufferFrame &bf, std::function<bool(Swip<BufferFrame> &)> callback)
{
   // TODO
}
// Helpers
// -------------------------------------------------------------------------------------
unsigned countInner(BTreeNode *node)
{
   if ( node->isLeaf )
      return 0;
   unsigned sum = 1;
   for ( unsigned i = 0; i < node->count; i++ )
      sum += countInner(node->getValue(i));
   sum += countInner(node->upper);
   return sum;
}
// -------------------------------------------------------------------------------------
unsigned countPages(BTreeNode *node)
{
   if ( node->isLeaf )
      return 1;
   unsigned sum = 1;
   for ( unsigned i = 0; i < node->count; i++ )
      sum += countPages(node->getValue(i));
   sum += countPages(node->upper);
   return sum;
}
// -------------------------------------------------------------------------------------
unsigned bytesFree(BTreeNode *node)
{
   if ( node->isLeaf )
      return node->freeSpaceAfterCompaction();
   unsigned sum = node->freeSpaceAfterCompaction();
   for ( unsigned i = 0; i < node->count; i++ )
      sum += bytesFree(node->getValue(i));
   sum += bytesFree(node->upper);
   return sum;
}
// -------------------------------------------------------------------------------------
unsigned height(BTreeNode *node)
{
   if ( node->isLeaf )
      return 1;
   return 1 + height(node->upper);
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