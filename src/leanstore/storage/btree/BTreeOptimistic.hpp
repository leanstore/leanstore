#pragma once
#include "Common.hpp"
#include "OptimisticNode.hpp"

using namespace std;
namespace optimistic {
using ub8 = uint64_t;

struct BTreeLeafBase : public NodeBase {
   static const PageType typeMarker = PageType::BTreeLeaf;
};

template<class Key, class Payload>
struct BTreeLeaf : public BTreeLeafBase {
   static const uint64_t pageSizeLeaf = 4 * 1024;
   static const uint64_t maxEntries = ((pageSizeLeaf - sizeof(NodeBase)) / (sizeof(Key) + sizeof(Payload))) - 1 /* slightly wasteful */;

   Key keys[maxEntries];
   Payload payloads[maxEntries];

   BTreeLeaf()
   {
       count = 0;
       type = typeMarker;
   }

   int64_t lowerBound(Key k)
   {
       unsigned lower = 0;
       unsigned upper = count;
       do {
           unsigned mid = ((upper - lower) / 2) + lower;
           if (k < keys[mid]) {
               if (!(mid <= upper)) {
                   return -1;
               }
               upper = mid;
           } else if (k > keys[mid]) {
               if (!(lower <= mid)) {
                   return -1;
               }
               lower = mid + 1;
           } else {
               return mid;
           }
       } while (lower < upper);
       return lower;
   }

   void insert(Key k, Payload p)
   {
       if (count) {
           unsigned pos = lowerBound(k);
           if (pos < count && keys[pos] == k) {
               // overwrite payload
               payloads[pos] = p;
               return;
           }
           memmove(keys + pos + 1, keys + pos, sizeof(Key) * (count - pos));
           memmove(payloads + pos + 1, payloads + pos, sizeof(Payload) * (count - pos));
           keys[pos] = k;
           payloads[pos] = p;
       } else {
           keys[0] = k;
           payloads[0] = p;
       }
       count++;
   }

   BTreeLeaf *split(Key &sep)
   {
       BTreeLeaf *newLeaf = new BTreeLeaf();
       newLeaf->count = count - (count / 2);
       count = count - newLeaf->count;
       memcpy(newLeaf->keys, keys + count, sizeof(Key) * newLeaf->count);
       memcpy(newLeaf->payloads, payloads + count, sizeof(Payload) * newLeaf->count);
       sep = keys[count - 1];
       return newLeaf;
   }
};

struct BTreeInnerBase : public NodeBase {
   static const PageType typeMarker = PageType::BTreeInner;
};

template<class Key>
struct BTreeInner : public BTreeInnerBase {
   static const uint64_t pageSizeInner = 4 * 1024;
   static const uint64_t maxEntries = ((pageSizeInner - sizeof(NodeBase)) / (sizeof(Key) + sizeof(NodeBase *))) - 1 /* slightly wasteful */;

   NodeBase *children[maxEntries];
   Key keys[maxEntries];

   BTreeInner()
   {
       count = 0;
       type = typeMarker;
   }

   int64_t lowerBound(Key k)
   {
       unsigned lower = 0;
       unsigned upper = count;
       do {
           unsigned mid = ((upper - lower) / 2) + lower;
           if (k < keys[mid]) {
               if (!(mid <= upper)) {
                   cout << " weird" << endl;
                   return -1;
               }
               upper = mid;
           } else if (k > keys[mid]) {
               if (!(lower <= mid)) {
                   cout << " weird" << endl;
                   return -1;
               }
               lower = mid + 1;
           } else {
               return mid;
           }
       } while (lower < upper);
       return lower;
   }

   BTreeInner *split(Key &sep)
   {
       BTreeInner *newInner = new BTreeInner();
       newInner->count = count - (count / 2);
       count = count - newInner->count - 1;
       sep = keys[count];
       memcpy(newInner->keys, keys + count + 1, sizeof(Key) * (newInner->count + 1));
       memcpy(newInner->children, children + count + 1, sizeof(NodeBase *) * (newInner->count + 1));
       return newInner;
   }

   void insert(Key k, NodeBase *child)
   {
       unsigned pos = lowerBound(k);
       memmove(keys + pos + 1, keys + pos, sizeof(Key) * (count - pos + 1));
       memmove(children + pos + 1, children + pos, sizeof(NodeBase *) * (count - pos + 1));
       keys[pos] = k;
       children[pos] = child;
       std::swap(children[pos], children[pos + 1]);
       count++;
   }
};

template<class Key, class Value>
struct BTree {
   atomic<NodeBase *> root;
   NodeBase root_lock;
   atomic<uint64_t > restarts_counter = 0;
   BTree()
   {
       root = new BTreeLeaf<Key, Value>();
   }

   void makeRoot(Key k, NodeBase *leftChild, NodeBase *rightChild)
   {
       auto inner = new BTreeInner<Key>();
       inner->count = 1;
       inner->keys[0] = k;
       inner->children[0] = leftChild;
       inner->children[1] = rightChild;
       root = inner;
   }

   void insert(Key k, Value v)
   {
      bool first = true;
      insert_start:
       if(!first) {
          restarts_counter++;
       } else {
          first = false;
       }
       ub8 root_version;

       if (!(root_version = readLockOrRestart(root_lock))) {
           goto insert_start;
       }

       ub8 version;
       ub8 parent_version = 0;
       uint16_t level = 0; // more for debugging

       NodeBase *node = root.load(), *parent_node = nullptr;

       if (!(version = readLockOrRestart(*node))) {
           goto insert_start;
       }

       while (node->type == PageType::BTreeInner) {
           auto inner = static_cast<BTreeInner<Key> *>(node);

           if (inner->count == inner->maxEntries - 1) { // Split inner eagerly
               if (parent_node) {
                   if (!upgradeToWriteLockOrRestart(*parent_node, parent_version)) {
                       goto insert_start;
                   }
               } else {
                   if (!upgradeToWriteLockOrRestart(root_lock, root_version)) {
                       goto insert_start;
                   }
               }
               if (!upgradeToWriteLockOrRestart(*node, version)) {
                   if (parent_node) {
                       writeUnlock(*parent_node);
                   } else {
                       writeUnlock(root_lock);
                   }
                   goto insert_start;
               }

               Key sep;
               BTreeInner<Key> *newInner = inner->split(sep);

               auto parent_inner = static_cast<BTreeInner<Key> *>(parent_node);

               if (parent_node) {
                   parent_inner->insert(sep, newInner);
                   writeUnlock(*node);
                   writeUnlock(*parent_node);
               } else {
                   makeRoot(sep, inner, newInner);
                   writeUnlock(*node);
                   writeUnlock(root_lock);
               }
               goto insert_start;
           }

           if (parent_node) {
               if (!readUnlockOrRestart(*parent_node, parent_version)) {
                   goto insert_start;
               }
           } else {
               if (!readUnlockOrRestart(root_lock, root_version)) {
                   goto insert_start;
               }
           }

           int64_t pos;
           if ((pos = inner->lowerBound(k)) == -1) {
               goto insert_start;
           }

           parent_node = node;
           parent_version = version;

           node = inner->children[pos];
           assert(node);

           if (level == 0 && !checkOrRestart(root_lock, root_version)) {
               goto insert_start;
           }

           if (!checkOrRestart(*parent_node, parent_version)) { // it refers to current node
               goto insert_start;
           }

           if (!(version = readLockOrRestart(*node))) {
               goto insert_start;
           }

           level++;
       }

       BTreeLeaf<Key, Value> *leaf = static_cast<BTreeLeaf<Key, Value> *>(node);

       if (parent_node) {
           if (!upgradeToWriteLockOrRestart(*parent_node, parent_version)) {
               goto insert_start;
           }
       } else {
           if (!upgradeToWriteLockOrRestart(root_lock, root_version)) {
               goto insert_start;
           }
       }
       if (!upgradeToWriteLockOrRestart(*node, version)) {
           goto insert_start;
       }

       if (leaf->count == leaf->maxEntries) {
           auto parent_inner = static_cast<BTreeInner<Key> *>(parent_node);
           // Leaf is full, split it
           Key sep;
           BTreeLeaf<Key, Value> *newLeaf = leaf->split(sep);
           if (parent_node) {
               parent_inner->insert(sep, newLeaf);
           } else {
               makeRoot(sep, leaf, newLeaf);
           }

           writeUnlock(*node);
           if (parent_node) {
               writeUnlock(*parent_node);
           } else {
               writeUnlock(root_lock);
           }
           goto insert_start;
       }
      // -------------------------------------------------------------------------------------
      if(rand() % 10 >=8){

         writeUnlock(*node);
         if (parent_node) {
            writeUnlock(*parent_node);
         } else {
            writeUnlock(root_lock);
         }
         goto insert_start;
      }
      // -------------------------------------------------------------------------------------
      leaf->insert(k, v);

       writeUnlock(*node);
       if (parent_node) {
           writeUnlock(*parent_node);
       } else {
           writeUnlock(root_lock);
       }
   }

   bool lookup(Key k, Value &result)
   {
      bool first = true;
      lookup_start:
         if(!first) {
            restarts_counter++;
         } else {
            first = false;
         }
       ub8 version;
       ub8 parent_version;
       bool is_root = true;
       NodeBase *node = root.load(), *parent_node;

       if (!(version = readLockOrRestart(*node))) {
           goto lookup_start;
       }

       while (node->type == PageType::BTreeInner) {
           BTreeInner<Key> *inner = static_cast<BTreeInner<Key> *>(node);

           if (is_root) {
               is_root = false;
           } else {
               if (!readUnlockOrRestart(*parent_node, parent_version)) {
                   goto lookup_start;
               }
           }

           int64_t pos;
           if ((pos = inner->lowerBound(k)) == -1) {
               goto lookup_start;
           }

           parent_node = node;
           parent_version = version;

           node = inner->children[pos];
           if (!checkOrRestart(*parent_node, parent_version)) { //refers to current node
               goto lookup_start;
           }

           if (!(version = readLockOrRestart(*node))) {
               goto lookup_start;
           }
       }

       if (!is_root) {
           readUnlockOrRestart(*parent_node, parent_version);
       }

       BTreeLeaf<Key, Value> *leaf = static_cast<BTreeLeaf<Key, Value> *>(node);
       int64_t pos;

       if ((pos = leaf->lowerBound(k)) == -1) {
           goto lookup_start;
       }

       if ((pos < leaf->count) && (leaf->keys[pos] == k)) {
           result = leaf->payloads[pos];

           if (!checkOrRestart(*node, version)) { //refers to current node
               goto lookup_start;
           }

           return true;
       }

       return false;
   }

   ~BTree() {
      cout << "restarts counter = " << restarts_counter << endl;
   }
};
}
