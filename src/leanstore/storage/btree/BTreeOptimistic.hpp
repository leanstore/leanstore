#pragma once
#include "leanstore/sync-primitives/OptimisticLock.hpp"
#include "leanstore/storage/buffer-manager/BufferFrame.hpp"
#include "leanstore/storage/buffer-manager/PageGuard.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace std;
namespace leanstore {
namespace btree {
enum class NodeType : u8 {
   BTreeInner = 1,
   BTreeLeaf = 2
};

struct NodeBase {
   NodeType type;
   u16 count;
   NodeBase() {}
};

struct BTreeLeafBase : public NodeBase {
   static const NodeType typeMarker = NodeType::BTreeLeaf;
};

using Node = NodeBase;
template<class Key, class Payload>
struct BTreeLeaf : public BTreeLeafBase {
   static const u64 maxEntries = ((PAGE_SIZE - sizeof(NodeBase) - sizeof(BufferFrame::Page)) / (sizeof(Key) + sizeof(Payload))) - 1 /* slightly wasteful */;
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
         if ( k < keys[mid] ) {
            if ( !(mid <= upper)) {
               throw RestartException();
            }
            upper = mid;
         } else if ( k > keys[mid] ) {
            if ( !(lower <= mid)) {
               throw RestartException();
            }
            lower = mid + 1;
         } else {
            return mid;
         }
      } while ( lower < upper );
      return lower;
   }

   void insert(Key k, Payload p)
   {
      if ( count ) {
         unsigned pos = lowerBound(k);
         if ( pos < count && keys[pos] == k ) {
            // overwrite page
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

   void split(Key &sep, BTreeLeaf &new_leaf)
   {
      new_leaf.count = count - (count / 2);
      count = count - new_leaf.count;
      memcpy(new_leaf.keys, keys + count, sizeof(Key) * new_leaf.count);
      memcpy(new_leaf.payloads, payloads + count, sizeof(Payload) * new_leaf.count);
      sep = keys[count - 1];
   }
};

struct BTreeInnerBase : public NodeBase {
   static const NodeType typeMarker = NodeType::BTreeInner;
};

template<class Key>
struct BTreeInner : public BTreeInnerBase {
   static const u64 maxEntries = ((PAGE_SIZE - sizeof(NodeBase) - sizeof(BufferFrame::Page)) / (sizeof(Key) + sizeof(NodeBase *))) - 1 /* slightly wasteful */;

   Swip<BTreeInner<Key>> children[maxEntries];
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
         if ( k < keys[mid] ) {
            if ( !(mid <= upper)) {
               throw RestartException();
            }
            upper = mid;
         } else if ( k > keys[mid] ) {
            if ( !(lower <= mid)) {
               throw RestartException();
            }
            lower = mid + 1;
         } else {
            return mid;
         }
      } while ( lower < upper );
      return lower;
   }

   void split(Key &sep, BTreeInner &new_inner) // BTreeInner *
   {
      new_inner.count = count - (count / 2);
      count = count - new_inner.count - 1;
      sep = keys[count];
      memcpy(new_inner.keys, keys + count + 1, sizeof(Key) * (new_inner.count + 1));
      memcpy(new_inner.children, children + count + 1, sizeof(Swip<BTreeInner<Key>>) * (new_inner.count + 1));
   }

   void insert(Key k, Swip<BTreeInner<Key>> child)
   {
      unsigned pos = lowerBound(k);
      memmove(keys + pos + 1, keys + pos, sizeof(Key) * (count - pos + 1));
      memmove(children + pos + 1, children + pos, sizeof(Swip<BTreeInner<Key>>) * (count - pos + 1));
      keys[pos] = k;
      children[pos] = child;
      std::swap(children[pos], children[pos + 1]);
      count++;
   }
};

template<class Key, class Value>
struct BTree {
   Swip<NodeBase> root_swip;
   OptimisticVersion root_lock = 0;
   atomic<u64> restarts_counter = 0; // for debugging

   BufferManager &buffer_manager;
   // -------------------------------------------------------------------------------------
   BTree(BufferFrame *root_bf)
           : root_swip(root_bf)
             , buffer_manager(*BMC::global_bf)
   {
   }
   // -------------------------------------------------------------------------------------
   void init()
   {
      SharedLock lock(root_lock);
      auto &root_bf = buffer_manager.resolveSwip(lock, root_swip.value);
      new(root_bf.page.dt) BTreeLeaf<Key, Value>();
   }
   // -------------------------------------------------------------------------------------
   void makeRoot(Key k, Swip<BTreeInner<Key>> leftChild, Swip<BTreeInner<Key>> rightChild)
   {
      auto new_root_inner = WritePageGuard<BTreeInner<Key>>::allocateNewPage();
      root_swip.value.swizzle(new_root_inner.bf);
      new_root_inner->count = 1;
      new_root_inner->keys[0] = k;
      new_root_inner->children[0] = leftChild;
      new_root_inner->children[1] = rightChild;
   }
   // -------------------------------------------------------------------------------------
   void insert(Key k, Value v)
   {
      while ( true ) {
         try {
            auto p_guard = ReadPageGuard<BTreeInner<Key>>::makeRootGuard(root_lock);
            ReadPageGuard<BTreeInner<Key>> c_guard(p_guard, root_swip);
            while ( c_guard->type == NodeType::BTreeInner ) {
               // -------------------------------------------------------------------------------------
               if ( c_guard->count == c_guard->maxEntries - 1 ) {
                  // Split inner eagerly
                  auto p_x_guard = WritePageGuard(std::move(p_guard));
                  auto c_x_guard =  WritePageGuard(std::move(c_guard));
                  Key sep;
                  auto new_inner = WritePageGuard<BTreeInner<Key>>::allocateNewPage();
                  c_guard->split(sep, new_inner.ref());
                  if ( p_guard )
                     p_guard->insert(sep, new_inner.bf);
                  else
                     makeRoot(sep, c_guard.bf, new_inner.bf);
                  // -------------------------------------------------------------------------------------
                  throw RestartException(); //restart
               }
               // -------------------------------------------------------------------------------------
               unsigned pos = c_guard->lowerBound(k);
               Swip<BTreeInner<Key>> &c_swip = c_guard->children[pos];
               // -------------------------------------------------------------------------------------
               p_guard = std::move(c_guard);
               c_guard = ReadPageGuard<BTreeInner<Key>>(p_guard, c_swip);
            }

            ReadPageGuard<BTreeLeaf<Key, Value>> leaf(std::move(c_guard));
            if ( leaf->count == leaf->maxEntries ) {
               auto p_x_guard = WritePageGuard(std::move(p_guard));
               auto c_x_guard =  WritePageGuard(std::move(leaf));
               // Leaf is full, split it
               Key sep;
               auto new_leaf = WritePageGuard<BTreeLeaf<Key, Value>>::allocateNewPage();
               leaf->split(sep, new_leaf.ref());
               if ( p_guard )
                  p_guard->insert(sep, new_leaf.bf);
               else
                  makeRoot(sep, leaf.bf, new_leaf.bf);

               throw RestartException();
            }
            // -------------------------------------------------------------------------------------
            auto c_x_lock = WritePageGuard(std::move(leaf));
            leaf->insert(k, v);
            return;
         } catch ( RestartException e ) {
            restarts_counter++;
         }
      }
   }
   // -------------------------------------------------------------------------------------
   bool lookup(Key k, Value &result)
   {
      while ( true ) {
         try {
            auto p_guard = ReadPageGuard<BTreeInner<Key>>::makeRootGuard(root_lock);
            ReadPageGuard<BTreeInner<Key>> c_guard(p_guard, root_swip);
            while ( c_guard->type == NodeType::BTreeInner ) {
               int64_t pos = c_guard->lowerBound(k);
               Swip<BTreeInner<Key>> &c_swip = c_guard->children[pos];
               // -------------------------------------------------------------------------------------
               p_guard = std::move(c_guard);
               c_guard = ReadPageGuard(p_guard, c_swip);
            }
            ReadPageGuard<BTreeLeaf<Key, Value>> leaf(std::move(c_guard));
            int64_t pos = leaf->lowerBound(k);
            if ((pos < leaf->count) && (leaf->keys[pos] == k)) {
               result = leaf->payloads[pos];
//               leaf.recheck();
               return true;
            }
//            leaf.recheck();
            return false;
         } catch ( RestartException e ) {
            restarts_counter++;
         }
      }
   }
   ~BTree()
   {
      cout << "restarts counter = " << restarts_counter << endl;
   }
};
}
}