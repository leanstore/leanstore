#pragma once
#include "leanstore/sync-primitives/OptimisticLock.hpp"
#include "leanstore/storage/buffer-manager/BufferFrame.hpp"
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

static const u64 pageSize = 16 * 1024;

using Node = NodeBase;
template<class Key, class Payload>
struct BTreeLeaf : public BTreeLeafBase {
   static const u64 maxEntries = ((pageSize - sizeof(NodeBase) - sizeof(BufferFrame::Page)) / (sizeof(Key) + sizeof(Payload))) - 1 /* slightly wasteful */;

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

   void split(Key &sep, BufferFrame &new_bf)
   {
      BTreeLeaf *newLeaf = new(new_bf.page.dt) BTreeLeaf();
      newLeaf->count = count - (count / 2);
      count = count - newLeaf->count;
      memcpy(newLeaf->keys, keys + count, sizeof(Key) * newLeaf->count);
      memcpy(newLeaf->payloads, payloads + count, sizeof(Payload) * newLeaf->count);
      sep = keys[count - 1];
   }
};

struct BTreeInnerBase : public NodeBase {
   static const NodeType typeMarker = NodeType::BTreeInner;
};

template<class Key>
struct BTreeInner : public BTreeInnerBase {
   static const u64 maxEntries = ((pageSize - sizeof(NodeBase) - sizeof(BufferFrame::Page)) / (sizeof(Key) + sizeof(NodeBase *))) - 1 /* slightly wasteful */;

   Swip children[maxEntries];
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

   void split(Key &sep, BufferFrame &new_inner_bf) // BTreeInner *
   {
      BTreeInner *newInner = new(new_inner_bf.page.dt) BTreeInner();
      newInner->count = count - (count / 2);
      count = count - newInner->count - 1;
      sep = keys[count];
      memcpy(newInner->keys, keys + count + 1, sizeof(Key) * (newInner->count + 1));
      memcpy(newInner->children, children + count + 1, sizeof(Swip) * (newInner->count + 1));
   }

   void insert(Key k, Swip child)
   {
      unsigned pos = lowerBound(k);
      memmove(keys + pos + 1, keys + pos, sizeof(Key) * (count - pos + 1));
      memmove(children + pos + 1, children + pos, sizeof(Swip) * (count - pos + 1));
      keys[pos] = k;
      children[pos] = child;
      std::swap(children[pos], children[pos + 1]);
      count++;
   }
};

template<class Key, class Value>
struct BTree {
   Swip root_swip;
   OptimisticLock root_lock = 0;
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
      auto &root_bf = buffer_manager.fixPage(lock, root_swip);
      new(root_bf.page.dt) BTreeLeaf<Key, Value>();
   }
   // -------------------------------------------------------------------------------------
   void makeRoot(Key k, Swip leftChild, Swip rightChild)
   {
      auto &new_root_bf = buffer_manager.accquireBufferFrame();
      root_swip.swizzle(&new_root_bf);
      auto inner = new(new_root_bf.page.dt) BTreeInner<Key>();
      inner->count = 1;
      inner->keys[0] = k;
      inner->children[0] = leftChild;
      inner->children[1] = rightChild;
   }
   // -------------------------------------------------------------------------------------
   void insert(Key k, Value v)
   {
      while ( true ) {
         try {
            SharedLock r_lock(root_lock);
            BufferFrame *c_bf = &buffer_manager.fixPage(r_lock, root_swip);
            auto c_node = reinterpret_cast<NodeBase *>(c_bf->page.dt);
            BTreeInner<Key> *p_node = nullptr;
            SharedLock c_lock(c_bf->header.lock);
            SharedLock p_lock = r_lock;
            while ( c_node->type == NodeType::BTreeInner ) {
               auto inner = static_cast<BTreeInner<Key> *>(c_node);
               // -------------------------------------------------------------------------------------
               if ( inner->count == inner->maxEntries - 1 ) {
                  // Split inner eagerly
                  ExclusiveLock p_x_lock(p_lock);
                  ExclusiveLock c_x_lock(c_lock);
                  Key sep;
                  auto &new_inner_bf = buffer_manager.accquireBufferFrame();
                  inner->split(sep, new_inner_bf);
                  if ( p_node )
                     p_node->insert(sep, &new_inner_bf);
                  else
                     makeRoot(sep, c_bf, &new_inner_bf);

                  throw RestartException(); //restart
               }
               // -------------------------------------------------------------------------------------
               p_lock.recheck(); // ^release^ parent before searching in the current node
               unsigned pos = inner->lowerBound(k);
               p_node = inner;
               Swip &c_swip = inner->children[pos];
               // -------------------------------------------------------------------------------------
               c_bf = &buffer_manager.fixPage(c_lock, c_swip);
               c_node = reinterpret_cast<NodeBase *>(c_bf->page.dt);
               // -------------------------------------------------------------------------------------
               c_lock.recheck();
               p_lock = c_lock;
               c_lock = SharedLock(c_bf->header.lock);
               assert(c_node);
            }

            BTreeLeaf<Key, Value> *leaf = static_cast<BTreeLeaf<Key, Value> *>(c_node);
            ExclusiveLock c_x_lock(c_lock);
            if ( leaf->count == leaf->maxEntries ) {
               ExclusiveLock p_x_lock(p_lock);  // TODO: correct ?
               // Leaf is full, split it
               Key sep;
               auto &new_leaf_bf = buffer_manager.accquireBufferFrame();
               leaf->split(sep, new_leaf_bf);
               if ( p_node )
                  p_node->insert(sep, &new_leaf_bf);
               else
                  makeRoot(sep, c_bf, &new_leaf_bf);

               throw RestartException();
            }
            // -------------------------------------------------------------------------------------
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
            SharedLock c_lock(root_lock);
            BufferFrame *c_bf = &buffer_manager.fixPage(c_lock, root_swip);
            SharedLock p_lock;
            auto c_node = reinterpret_cast<NodeBase *>(c_bf->page.dt);

            while ( c_node->type == NodeType::BTreeInner ) {
               BTreeInner<Key> *inner = static_cast<BTreeInner<Key> *>(c_node);

               if ( p_lock ) {
                  p_lock.recheck();
               }

               int64_t pos = inner->lowerBound(k);
               Swip &c_swip = inner->children[pos];
               // -------------------------------------------------------------------------------------
               c_bf = &buffer_manager.fixPage(c_lock, c_swip);
               c_node = reinterpret_cast<NodeBase *>(c_bf->page.dt);
               // -------------------------------------------------------------------------------------
               c_lock.recheck();
               p_lock = c_lock;
               c_lock = SharedLock(c_bf->header.lock);
            }

            if ( p_lock ) {
               p_lock.recheck();
            }

            BTreeLeaf<Key, Value> *leaf = static_cast<BTreeLeaf<Key, Value> *>(c_node);
            int64_t pos = leaf->lowerBound(k);
            if ((pos < leaf->count) && (leaf->keys[pos] == k)) {
               result = leaf->payloads[pos];
               c_lock.recheck();
               return true;
            }
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