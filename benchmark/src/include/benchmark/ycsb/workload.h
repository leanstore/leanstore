#pragma once

#include "benchmark/adapters/leanstore_adapter.h"
#include "benchmark/utils/misc.h"
#include "benchmark/utils/rand.h"
#include "benchmark/ycsb/config.h"
#include "benchmark/ycsb/schema.h"

#include "share_headers/config.h"
#include "share_headers/db_types.h"
#include "share_headers/logger.h"
#include "tbb/blocked_range.h"

#include <algorithm>
#include <functional>
#include <span>
#include <variant>

namespace ycsb {

using WorkerLocalPayloads = std::vector<std::unique_ptr<uint8_t[]>>;

class YCSBWorkloadInterface {
 public:
  virtual ~YCSBWorkloadInterface()                                                      = default;
  virtual auto CountEntries() -> uint64_t                                               = 0;
  virtual void LoadInitialData(UInteger w_id, const tbb::blocked_range<Integer> &range) = 0;
  virtual void ExecuteTransaction(UInteger w_id)                                        = 0;

  static auto PayloadSize() -> uint64_t {
    if (FLAGS_ycsb_random_payload) {
      return RoundUp(FLAGS_ycsb_payload_size_align,
                     RandomGenerator::GetRandU64(FLAGS_ycsb_payload_size, FLAGS_ycsb_max_payload_size));
    }
    return FLAGS_ycsb_payload_size;
  }
};

/**
 * For Adapter without BlobRep, use this struct for arbitrary-sized payload workloads
 */
template <template <typename> class AdapterType, class YCSBRelation>
struct YCSBWorkloadNoBlobRep : public YCSBWorkloadInterface {
  // Run-time
  AdapterType<YCSBRelation> relation;
  ZipfGenerator zipf_generator;
  WorkerLocalPayloads payloads;
  static constexpr bool ENABLE_BLOB_REP = false;

  // YCSB settings
  const Integer record_count;  // Number of records
  const UInteger read_ratio;   // Read ratio

  template <typename... Params>
  YCSBWorkloadNoBlobRep(Integer initial_record_cnt, UInteger required_read_ratio, double zipf_theta,
                        bool use_blob_register, WorkerLocalPayloads &payloads, Params &&...params)
      : relation(AdapterType<YCSBRelation>(std::forward<Params>(params)...)),
        zipf_generator(zipf_theta, initial_record_cnt),
        payloads(std::move(payloads)),
        record_count(initial_record_cnt),
        read_ratio(std::min(required_read_ratio, static_cast<UInteger>(99))) {
    Ensure((FLAGS_ycsb_payload_size > BLOB_NORMAL_PAYLOAD) || (!use_blob_register));
    if (FLAGS_ycsb_random_payload) {
      Ensure(FLAGS_ycsb_payload_size > BLOB_NORMAL_PAYLOAD);
    } else {
      Ensure(FLAGS_ycsb_payload_size == FLAGS_ycsb_max_payload_size);
    }
    Ensure((0 <= zipf_theta) && (zipf_theta <= 1));
    Ensure(read_ratio <= 99);
  }

  auto CountEntries() -> uint64_t override { return relation.Count(); }

  void LoadInitialData(UInteger w_id, const tbb::blocked_range<Integer> &range) override {
    auto payload = payloads[w_id].get();
    auto record  = reinterpret_cast<YCSBRelation *>(payload);

    for (auto key = range.begin(); key < range.end(); key++) {
      // Generate key-value
      auto r_key             = YCSBKey{static_cast<UInteger>(key)};
      auto payload_sz        = YCSBWorkloadInterface::PayloadSize();
      record->payload.length = payload_sz;
      RandomGenerator::GetRandRepetitiveString(reinterpret_cast<uint8_t *>(record->payload.data), 100UL, payload_sz);
      relation.Insert({r_key}, *record);
    }
  }

  void ExecuteTransaction(UInteger w_id) override {
    auto access_key  = static_cast<UInteger>(zipf_generator.Rand());
    auto is_read_txn = RandomGenerator::GetRandU64(0, 100) <= read_ratio;
    auto payload     = payloads[w_id].get();
    auto record      = reinterpret_cast<YCSBRelation *>(payload);

    // Fstat experiment
    if (FLAGS_ycsb_benchmark_fstat) {
      for (auto key = access_key; key < access_key + 10; key++) { relation.FileStat({key}); }
      return;
    }

    // Transaction without Blob rep
    if (is_read_txn) {
      relation.LookUp({access_key}, [&](const YCSBRelation &rec) {
        std::memcpy(payload, const_cast<YCSBRelation &>(rec).payload.data, rec.payload.length);
      });
    } else {
      auto payload_sz        = YCSBWorkloadInterface::PayloadSize();
      record->payload.length = payload_sz;
      RandomGenerator::GetRandRepetitiveString(reinterpret_cast<uint8_t *>(record->payload.data), 100UL, payload_sz);
      relation.Update({access_key}, *record);
    }
  }
};

template <template <typename> class AdapterType, class YCSBRelation>
struct YCSBWorkload : public YCSBWorkloadInterface {
  // Run-time
  AdapterType<YCSBRelation> relation;
  ZipfGenerator zipf_generator;
  WorkerLocalPayloads payloads;

  // YCSB settings
  const Integer record_count;  // Number of records
  const UInteger read_ratio;   // Read ratio
  const bool enable_blob_rep;  // Whether to use custom Blob representative format or not for Blob workloads
                               // This depends on whether the DB engine requires extern Blob creation API
                               // e.g. It's possible to insert Blob directly into MySQL using SQL `insert` command

  template <typename... Params>
  YCSBWorkload(Integer initial_record_cnt, UInteger required_read_ratio, double zipf_theta, bool use_blob_register,
               WorkerLocalPayloads &payloads, Params &&...params)
      : relation(AdapterType<YCSBRelation>(std::forward<Params>(params)...)),
        zipf_generator(zipf_theta, initial_record_cnt),
        payloads(std::move(payloads)),
        record_count(initial_record_cnt),
        read_ratio(std::min(required_read_ratio, static_cast<UInteger>(99))),
        enable_blob_rep((FLAGS_ycsb_payload_size > BLOB_NORMAL_PAYLOAD) && (use_blob_register)) {
    if (FLAGS_ycsb_random_payload) {
      Ensure(FLAGS_ycsb_payload_size > BLOB_NORMAL_PAYLOAD);
    } else {
      FLAGS_ycsb_max_payload_size = FLAGS_ycsb_payload_size;
    }
    assert((0 <= zipf_theta) && (zipf_theta <= 1));
    assert(read_ratio <= 99);
  }

  auto CountEntries() -> uint64_t override { return relation.Count(); }

  void LoadInitialData(UInteger w_id, const tbb::blocked_range<Integer> &range) override {
    auto payload = payloads[w_id].get();

    for (auto key = range.begin(); key < range.end(); key++) {
      // Generate key-value
      auto r_key      = YCSBKey{static_cast<UInteger>(key)};
      auto payload_sz = YCSBWorkloadInterface::PayloadSize();
      RandomGenerator::GetRandRepetitiveString(payload, 100UL, payload_sz);

      // If the value is Blob, then we register it first
      if (enable_blob_rep) {
        auto blob_rep = relation.RegisterBlob({payload, payload_sz}, {}, false);
        relation.InsertRawPayload({r_key}, blob_rep);
      } else {
        relation.Insert({r_key}, *reinterpret_cast<YCSBRelation *>(payload));
      }
    }
  }

  void ExecuteTransaction(UInteger w_id) override {
    auto access_key  = static_cast<UInteger>(zipf_generator.Rand());
    auto is_read_txn = RandomGenerator::GetRandU64(0, 100) <= read_ratio;
    auto payload     = payloads[w_id].get();

    // Transaction without Blob rep
    if (!enable_blob_rep) {
      if (is_read_txn) {
        relation.LookUp({access_key}, [&](const YCSBRelation &rec) {
          std::memcpy(payload, const_cast<YCSBRelation &>(rec).my_payload.Data(), FLAGS_ycsb_max_payload_size);
        });
      } else {
        auto payload_sz = YCSBWorkloadInterface::PayloadSize();
        RandomGenerator::GetRandRepetitiveString(payload, 100UL, payload_sz);
        relation.UpdateInPlace({access_key},
                               [&](YCSBRelation &rec) { std::memcpy(rec.my_payload.Data(), payload, payload_sz); });
      }
      return;
    }
    Ensure(enable_blob_rep);
    uint8_t blob_rep[MAX_BLOB_REPRESENT_SIZE];
    uint64_t blob_rep_size = 0;

    // Fstat experiment
    if (FLAGS_ycsb_benchmark_fstat) {
      int cnt = 0;
      relation.Scan({access_key}, [&](const auto &, const YCSBRelation &rec) {
        blob_rep_size = rec.PayloadSize();
        std::memcpy(blob_rep, const_cast<YCSBRelation &>(rec).my_payload.Data(), rec.PayloadSize());
        return (++cnt < 10);
      });
      return;
    }

    if (is_read_txn) {
      // Read Blob Rep
      auto found = relation.LookUp({access_key}, [&](const YCSBRelation &rec) {
        blob_rep_size = rec.PayloadSize();
        std::memcpy(blob_rep, const_cast<YCSBRelation &>(rec).my_payload.Data(), rec.PayloadSize());
      });
      Ensure(found);
      // Read Blob data using Blob Rep
      relation.LoadBlob(
        blob_rep,
        [&](std::span<const u8> blob_data) {
          Ensure((blob_data.size() >= FLAGS_ycsb_payload_size) && (blob_data.size() <= FLAGS_ycsb_max_payload_size));
          std::memcpy(payload, blob_data.data(), blob_data.size());
        },
        false);
    } else {
      // Generate a new random BLOB
      auto payload_sz = YCSBWorkloadInterface::PayloadSize();
      RandomGenerator::GetRandRepetitiveString(payload, 128UL, payload_sz);
      auto new_blob_rep = relation.RegisterBlob({payloads[w_id].get(), payload_sz}, {}, false);

      // UpdateInPlace() is slightly cheaper than Update()/UpdateRawPayload(),
      //  but only works when payload size doesn't change
      if (new_blob_rep.size() == blob_rep_size) [[likely]] {
        relation.UpdateInPlace({access_key}, [&](YCSBRelation &rec) {
          std::memcpy(blob_rep, const_cast<YCSBRelation &>(rec).my_payload.Data(), rec.PayloadSize());
          std::memcpy(rec.my_payload.Data(), new_blob_rep.data(), new_blob_rep.size());
        });
      } else {
        relation.UpdateRawPayload({access_key}, new_blob_rep, [&](const YCSBRelation &prev) {
          std::memcpy(blob_rep, const_cast<YCSBRelation &>(prev).my_payload.Data(), prev.PayloadSize());
        });
      }

      relation.RemoveBlob(blob_rep);
    }
  }
};

}  // namespace ycsb