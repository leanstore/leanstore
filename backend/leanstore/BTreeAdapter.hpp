#include "Units.hpp"
#include "leanstore/storage/btree/fs/BTreeOptimistic.hpp"
#include "leanstore/storage/btree/vs/BTreeVS.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
unsigned fold(uint8_t* writer, const s32& x)
{
  *reinterpret_cast<u32*>(writer) = __builtin_bswap32(x ^ (1ul << 31));
  return sizeof(x);
}

unsigned fold(uint8_t* writer, const s64& x)
{
  *reinterpret_cast<u64*>(writer) = __builtin_bswap64(x ^ (1ull << 63));
  return sizeof(x);
}

unsigned fold(uint8_t* writer, const u64& x)
{
  *reinterpret_cast<u64*>(writer) = __builtin_bswap64(x);
  return sizeof(x);
}

unsigned fold(uint8_t* writer, const u32& x)
{
  *reinterpret_cast<u32*>(writer) = __builtin_bswap32(x);
  return sizeof(x);
}
// -------------------------------------------------------------------------------------
template <typename Key, typename Payload>
struct BTreeInterface {
  virtual bool lookup(Key k, Payload& v) = 0;
  virtual void insert(Key k, Payload& v) = 0;
  virtual void update(Key k, Payload& v) = 0;
};
// -------------------------------------------------------------------------------------
// template <typename Record>
// struct DataStructureInterface {
//   void insert(const Record& r);
//   bool lookup(const typename Record::Key& k);
//   template <class Fn>
//   void update(const typename Record::Key& k, const Fn& fn);
//   template <class Fn>
//   void scan(const typename Record::Key& k, const Fn& fn, std::function<void()> undo);
// };
// // -------------------------------------------------------------------------------------
// template <typename Record>
// struct VSBTreeAdapter : DataStructureInterface<Record> {
//   leanstore::btree::vs::BTree& btree;
//   VSBTreeAdapter(leanstore::btree::vs::BTree& btree) : btree(btree) {}
//   void insert(const Record &r) {
//     u8 key_bytes[Record::maxFoldSize()];
//     btree.insert(key_bytes, fold(key_bytes, k), sizeof(v), reinterpret_cast<u8*>(&v));
//   }
// }
// -------------------------------------------------------------------------------------
template <typename Key, typename Payload>
struct BTreeVSAdapter : BTreeInterface<Key, Payload> {
  leanstore::btree::vs::BTree& btree;

  BTreeVSAdapter(leanstore::btree::vs::BTree& btree) : btree(btree) {}

  bool lookup(Key k, Payload& v) override
  {
    u8 key_bytes[sizeof(Key)];
    return btree.lookupOne(key_bytes, fold(key_bytes, k), [&](const u8* payload, u16 payload_length) { memcpy(&v, payload, payload_length); });
  }
  void insert(Key k, Payload& v) override
  {
    u8 key_bytes[sizeof(Key)];
    btree.insert(key_bytes, fold(key_bytes, k), sizeof(v), reinterpret_cast<u8*>(&v));
  }
  void update(Key k, Payload& v) override
  {
    u8 key_bytes[sizeof(Key)];
    btree.updateSameSize(key_bytes, fold(key_bytes, k), [&](u8* payload, u16 payload_length) { memcpy(payload, &v, payload_length); });
  }
};
// -------------------------------------------------------------------------------------
template <typename Key, typename Payload>
struct BTreeFSAdapter : BTreeInterface<Key, Payload> {
  leanstore::btree::fs::BTree<Key, Payload>& btree;
  BTreeFSAdapter(leanstore::btree::fs::BTree<Key, Payload>& btree) : btree(btree) { btree.printFanoutInformation(); }
  bool lookup(Key k, Payload& v) override { return btree.lookup(k, v); }
  void insert(Key k, Payload& v) override { btree.insert(k, v); }
  void update(Key k, Payload& v) override { btree.insert(k, v); }
};
// -------------------------------------------------------------------------------------
template <u64 size>
struct BytesPayload {
  u8 value[size];
  BytesPayload() {}
  bool operator==(BytesPayload& other) { return (std::memcmp(value, other.value, sizeof(value)) == 0); }
  bool operator!=(BytesPayload& other) { return !(operator==(other)); }
  BytesPayload(const BytesPayload& other) { std::memcpy(value, other.value, sizeof(value)); }
  BytesPayload& operator=(const BytesPayload& other)
  {
    std::memcpy(value, other.value, sizeof(value));
    return *this;
  }
};
}  // namespace leanstore
