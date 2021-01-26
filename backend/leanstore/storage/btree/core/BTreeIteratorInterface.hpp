#pragma once
#include "BTreeInterface.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace btree
{
// -------------------------------------------------------------------------------------
// Interface
class BTreeIteratorInterface
{
  public:
   // >= key
   virtual OP_RESULT seek(Slice key) = 0;
   // <= key
   virtual OP_RESULT seekForPrev(Slice key) = 0;
   virtual OP_RESULT seekExact(Slice key) = 0;
   virtual OP_RESULT next() = 0;
   virtual OP_RESULT prev() = 0;
   virtual bool isKeyEqualTo(Slice key) = 0;
};
class BTreeOptimisticIteratorInterface : public BTreeIteratorInterface  // Can jump
{
  public:
   virtual void key(std::function<void(Slice key)> cb) = 0;
   virtual void keyWithoutPrefix(std::function<void(Slice key)> cb) = 0;
   virtual void keyPrefix(std::function<void(Slice key)> cb) = 0;
   virtual void value(std::function<void(Slice key)> cb) = 0;
};
// -------------------------------------------------------------------------------------
class BTreePessimisticIteratorInterface : public BTreeIteratorInterface
{
  public:
   virtual Slice key() = 0;
   virtual Slice keyWithoutPrefix() = 0;
   virtual Slice keyPrefix() = 0;
   virtual Slice value() = 0;
};
using BTreeSharedIteratorInterface = BTreePessimisticIteratorInterface;
// -------------------------------------------------------------------------------------
class BTreeExclusiveIteratorInterface : public BTreePessimisticIteratorInterface
{
  public:
   virtual OP_RESULT seekToInsert(Slice key) = 0;
   virtual OP_RESULT remove(Slice key) = 0;
   virtual OP_RESULT insert(Slice key, Slice Value) = 0;
   virtual OP_RESULT split() = 0;
};
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
// -------------------------------------------------------------------------------------
