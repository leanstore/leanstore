#include "BTreeSlotted.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace std;

// -------------------------------------------------------------------------------------
BTree::BTree()
        : root(BTreeNode::makeLeaf()) {}
bool BTree::lookup(u8 *key, unsigned keyLength, ValueType &result)
{
   BTreeNode *node = root;
   while ( node->isInner())
      node = node->lookupInner(key, keyLength);
   int pos = node->lowerBound<true>(key, keyLength);
   if ( pos != -1 ) {
      result = node->getValue(pos);
      return true;
   }
   return false;
}
// -------------------------------------------------------------------------------------
void BTree::lookupInner(u8 *key, unsigned keyLength)
{
   BTreeNode *node = root;
   while ( node->isInner())
      node = node->lookupInner(key, keyLength);
   assert(node);
}
// -------------------------------------------------------------------------------------
void BTree::splitNode(BTreeNode *node, BTreeNode *parent, u8 *key, unsigned keyLength)
{
   if ( !parent ) {
      // create new root
      parent = BTreeNode::makeInner();
      parent->upper = node;
      root = parent;
   }
   BTreeNode::SeparatorInfo sepInfo = node->findSep();
   unsigned spaceNeededParent = BTreeNode::spaceNeeded(sepInfo.length, parent->prefixLength);
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
void BTree::insert(u8 *key, unsigned keyLength, ValueType value)
{
   BTreeNode *node = root;
   BTreeNode *parent = nullptr;
   while ( node->isInner()) {
      parent = node;
      node = node->lookupInner(key, keyLength);
   }
   if ( node->insert(key, keyLength, value))
      return;
   // no more space, need to split
   splitNode(node, parent, key, keyLength);
   insert(key, keyLength, value);
}
// -------------------------------------------------------------------------------------
bool BTree::remove(u8 *key, unsigned keyLength)
{
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
BTree::~BTree()
{
   root->destroy();
}
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
   cout << "nodes:" << cnt << " innerNodes:" << countInner(root) << " space:" << (cnt * BTreeNodeHeader::pageSize) / (float) totalSize << " height:" << height(root) << " rootCnt:" << root->count << " bytesFree:" << bytesFree(root) << endl;
}
// -------------------------------------------------------------------------------------
