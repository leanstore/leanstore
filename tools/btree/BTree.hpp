#pragma once
#include <random>
#include "JumpMU.hpp"
#include "Primitives.hpp"

namespace libgcc
{
struct NodeBase {
  PageType type;
  uint16_t count;
  atomic<uint64_t> version;
  NodeBase() : version(8) {}
};

using Node = NodeBase;

struct BTreeLeafBase : public NodeBase {
  static const PageType typeMarker = PageType::BTreeLeaf;
};

template <class Key, class Payload>
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

  BTreeLeaf* split(Key& sep)
  {
    BTreeLeaf* newLeaf = new BTreeLeaf();
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

template <class Key>
struct BTreeInner : public BTreeInnerBase {
  static const uint64_t pageSizeInner = 4 * 1024;
  static const uint64_t maxEntries = ((pageSizeInner - sizeof(NodeBase)) / (sizeof(Key) + sizeof(NodeBase*))) - 1 /* slightly wasteful */;

  NodeBase* children[maxEntries];
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
          assert(false);
          jumpmu::restore();
        }
        upper = mid;
      } else if (k > keys[mid]) {
        if (!(lower <= mid)) {
          assert(false);
          jumpmu::restore();
        }
        lower = mid + 1;
      } else {
        return mid;
      }
    } while (lower < upper);
    return lower;
  }

  BTreeInner* split(Key& sep)
  {
    BTreeInner* newInner = new BTreeInner();
    newInner->count = count - (count / 2);
    count = count - newInner->count - 1;
    sep = keys[count];
    memcpy(newInner->keys, keys + count + 1, sizeof(Key) * (newInner->count + 1));
    memcpy(newInner->children, children + count + 1, sizeof(NodeBase*) * (newInner->count + 1));
    return newInner;
  }

  void insert(Key k, NodeBase* child)
  {
    unsigned pos = lowerBound(k);
    memmove(keys + pos + 1, keys + pos, sizeof(Key) * (count - pos + 1));
    memmove(children + pos + 1, children + pos, sizeof(NodeBase*) * (count - pos + 1));
    keys[pos] = k;
    children[pos] = child;
    std::swap(children[pos], children[pos + 1]);
    count++;
  }
};

template <class Key, class Value>
struct BTree {
  atomic<NodeBase*> root;
  lock_t root_version;
  atomic<u64> restarts_counter = 0;

  BTree()
  {
    cout << BTreeLeaf<Key, Value>::maxEntries << endl;
    cout << BTreeInner<Key>::maxEntries << endl;
    root = new BTreeLeaf<Key, Value>();
    root_version = 0;
  }
  // -------------------------------------------------------------------------------------
  void makeRoot(Key k, NodeBase* leftChild, NodeBase* rightChild)
  {
    auto inner = new BTreeInner<Key>();
    inner->count = 1;
    inner->keys[0] = k;
    inner->children[0] = leftChild;
    inner->children[1] = rightChild;
    root = inner;
    cout << "make root" << endl;
  }
  // -------------------------------------------------------------------------------------
  void insert(Key k, Value v)
  {
    assert(jumpmu::checkpoint_counter == 0);
    while (true) {
      jumpmuTry()
      {
        SharedLock r_lock(root_version);
        // -------------------------------------------------------------------------------------
        NodeBase* c_node = root;
        BTreeInner<Key>* p_node = nullptr;
        SharedLock p_lock(root_version);
        SharedLock c_lock(c_node->version);
        while (c_node->type == PageType::BTreeInner) {
          auto inner = static_cast<BTreeInner<Key>*>(c_node);
          // -------------------------------------------------------------------------------------
          if (inner->count == inner->maxEntries - 1) {
            // Split inner eagerly
            ExclusiveLock p_x_lock(p_lock);
            ExclusiveLock c_x_lock(c_lock);
            Key sep;
            BTreeInner<Key>* newInner = inner->split(sep);
            if (p_node != nullptr)
              p_node->insert(sep, newInner);
            else {
              makeRoot(sep, inner, newInner);
            }

            BTreeInner<Key>* new_root = static_cast<BTreeInner<Key>*>(root.load());
            jumpmu::restore();
          }
          // -------------------------------------------------------------------------------------
          unsigned pos = inner->lowerBound(k);
          auto ptr = inner->children[pos];

          p_node = inner;
          p_lock.recheck();
          p_lock = c_lock;

          c_node = ptr;
          c_lock = SharedLock(c_node->version);
          // -------------------------------------------------------------------------------------
          assert(c_node);
          // -------------------------------------------------------------------------------------
        }
        BTreeLeaf<Key, Value>* leaf = static_cast<BTreeLeaf<Key, Value>*>(c_node);
        ExclusiveLock p_x_lock(p_lock);
        ExclusiveLock c_x_lock(c_lock);
        if (leaf->count == leaf->maxEntries) {
          // Leaf is full, split it
          Key sep;
          BTreeLeaf<Key, Value>* newLeaf = leaf->split(sep);
          if (p_node != nullptr)
            p_node->insert(sep, newLeaf);
          else {
            makeRoot(sep, leaf, newLeaf);
          }
          jumpmu::restore();
        }
        leaf->insert(k, v);
        {
          int64_t pos = leaf->lowerBound(k);
          assert((pos < leaf->count) && (leaf->keys[pos] == k));
        }
        jumpmu_return;
      }
      jumpmuCatch()
      {
        restarts_counter++;
      }
    }
    assert(jumpmu::checkpoint_counter == 0);
  }
  bool lookup(Key k, Value& result)
  {
    while (true) {
      jumpmuTry()
      {
        NodeBase* c_node = root.load();

        SharedLock c_lock(c_node->version);
        SharedLock p_lock;

        while (c_node->type == PageType::BTreeInner) {
          BTreeInner<Key>* inner = static_cast<BTreeInner<Key>*>(c_node);

          if (p_lock) {
            p_lock.recheck();
          }

          int64_t pos = inner->lowerBound(k);
          c_node = inner->children[pos];
          if (p_lock) {
            p_lock.recheck();
          }
          c_lock.recheck();
          p_lock = c_lock;
          c_lock = SharedLock(c_node->version);
        }

        if (p_lock) {
          p_lock.recheck();
        }

        BTreeLeaf<Key, Value>* leaf = static_cast<BTreeLeaf<Key, Value>*>(c_node);
        int64_t pos = leaf->lowerBound(k);
        if ((pos < leaf->count) && (leaf->keys[pos] == k)) {
          result = leaf->payloads[pos];
          c_lock.recheck();
          jumpmu_return true;
        }
        jumpmu_return false;
      }
      jumpmuCatch() { restarts_counter++; }
    }
  }
  ~BTree() { cout << "restarts counter = " << restarts_counter << endl; }
};
}  // namespace libgcc
