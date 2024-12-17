#pragma once
#include "IMNode.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/storage/buffer-manager/DTRegistry.hpp"

namespace leanstore {
namespace storage {
namespace inmemory {

class IMStore : public KVInterface {
public:
   struct Config {
      bool enable_concurrency = true;
   };

   IMStore();
   void create(DTID dtid, Config config);

   // KVInterface implementation
   virtual OP_RESULT lookup(u8* key, u16 key_length, 
                          function<void(const u8*, u16)> payload_callback) override;
   virtual OP_RESULT insert(u8* key, u16 key_length, 
                          u8* payload, u16 payload_length) override;
   virtual OP_RESULT update(u8* key, u16 key_length,
                          function<void(u8*)> callback, u16 payload_length) override;
   virtual OP_RESULT remove(u8* key, u16 key_length) override;

   static DTRegistry::DTMeta getMeta();

private:
   DTID dt_id;
   Config config;
   std::unique_ptr<IMNode> root;
};

}}} // namespace 