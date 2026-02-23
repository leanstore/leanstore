#pragma once

#include "common/delta.h"
#include "common/typedefs.h"

#include <functional>
#include <span>

namespace leanstore {

// All functions should be idempotent + have no side-effect
using AccessPayloadFunc = std::function<void(std::span<const u8>)>;
using ModifyPayloadFunc = std::function<void(std::span<u8>)>;
using AccessRecordFunc  = std::function<bool(std::span<u8>, std::span<u8>)>;

enum class ComparisonOperator : u8 {
  MEMCMP,
  BLOB_HANDLER,
  BLOB_LOOKUP,
};

struct ComparisonLambda {
  ComparisonOperator op;
  std::function<int(const void *, const void *, size_t)> func;
};

enum class OpResult : u8 { OK = 0, NOT_FOUND = 1, ABORT_TX = 2, STOP_SCAN = 3 };

class KVInterface {
 public:
  virtual ~KVInterface() = default;

  // Insertion should be append-biases or not
  virtual void SetComparisonOperator(ComparisonLambda cmp) = 0;

  // -------------------------------------------------------------------------------------
  virtual auto LookUp(std::span<u8> key, const AccessPayloadFunc &read_cb) -> OpResult                            = 0;
  virtual auto Insert(std::span<u8> key, std::span<const u8> payload) -> OpResult                                 = 0;
  virtual auto Remove(std::span<u8> key) -> OpResult                                                              = 0;
  virtual auto Update(std::span<u8> key, std::span<const u8> payload, const AccessPayloadFunc &func) -> OpResult  = 0;
  virtual auto UpdateInPlace(std::span<u8> key, const ModifyPayloadFunc &func, FixedSizeDelta *delta) -> OpResult = 0;
  virtual auto ScanAscending(std::span<u8> key, const AccessRecordFunc &fn) -> OpResult                           = 0;
  virtual auto ScanDescending(std::span<u8> key, const AccessRecordFunc &fn) -> OpResult                          = 0;
  virtual auto CountEntries() -> u64                                                                              = 0;
  virtual auto SizeInMB() -> float                                                                                = 0;

  // -------------------------------------------------------------------------------------
  // TODO(XXX): Not yet support Serializable for LookUpBlob
  virtual auto LookUpBlob(std::span<const u8> blob_payload, const ComparisonLambda &cmp,
                          const AccessPayloadFunc &read_cb) -> bool = 0;
};

using InternalCatalog = std::vector<KVInterface *>;

}  // namespace leanstore
