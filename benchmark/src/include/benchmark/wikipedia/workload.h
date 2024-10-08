#pragma once

#include "benchmark/adapters/leanstore_adapter.h"
#include "benchmark/utils/misc.h"
#include "benchmark/wikipedia/config.h"
#include "benchmark/wikipedia/schema.h"
#include "common/rand.h"
#include "leanstore/leanstore.h"

#include "share_headers/config.h"
#include "share_headers/csv.h"
#include "share_headers/db_types.h"
#include "share_headers/logger.h"
#include "tbb/blocked_range.h"

#include <algorithm>

#define MAX_INROW_SIZE leanstore::schema::MAX_INROW_SIZE

namespace wiki {

template <template <typename> class AdapterType, class Relation>
struct WikipediaReadOnly {
  AdapterType<Relation> relation;
  const bool enable_blob_rep;

  // Workload characteristics & Random distribution
  std::vector<std::pair<uint64_t, uint32_t>> characteristic;
  std::vector<uint64_t> dist;
  uint64_t max_dist{0};

  // Statistics
  std::atomic<uint32_t> inrow_count;
  std::atomic<uint32_t> offrow_count;

  template <typename... Params>
  explicit WikipediaReadOnly(bool enable_blob_rep, Params &&...params)
      : relation(AdapterType<Relation>(std::forward<Params>(params)...)),
        enable_blob_rep(enable_blob_rep),
        inrow_count(0),
        offrow_count(0) {
    io::CSVReader<2> in(FLAGS_wiki_workload_config_path.c_str());
    in.read_header(io::ignore_extra_column, "page_len", "monthly_views");
    uint64_t record_len_in_bytes = 0;
    uint32_t view_count          = 0;
    while (in.read_row(record_len_in_bytes, view_count)) {
      characteristic.emplace_back(RoundUp(64, record_len_in_bytes), view_count);
      max_dist += view_count;
    }
    dist.resize(characteristic.size(), 0);
    for (uint32_t idx = 0; idx < characteristic.size(); idx++) {
      dist[idx] = (idx > 0) ? dist[idx - 1] + characteristic[idx].second : characteristic[idx].second;
    }
  }

  // -------------------------------------------------------------------------------------
  auto RequiredNumberOfEntries() -> uint64_t { return characteristic.size(); }

  auto CountEntries() -> uint64_t { return relation.Count(); }

  auto DetermineSearchKey() -> uint32_t {
    auto perc  = RandomGenerator::GetRandU64(0, max_dist);
    auto lower = 1UL;
    auto upper = characteristic.size();
    while (lower < upper) {
      auto mid = ((upper - lower) / 2) + lower;
      if (dist[mid] > perc) {
        upper = mid;
      } else {
        lower = mid + 1;
      }
    }
    Ensure(dist[lower] >= perc);
    return lower;
  }

  // -------------------------------------------------------------------------------------
  void LoadInitialDataWithoutBlobRep(const tbb::blocked_range<UInteger> &range) {
    Ensure(!enable_blob_rep);

    for (auto key = range.begin(); key < range.end(); key++) {
      // Generate key-value on aligned memory block
      auto payload_sz = characteristic[key - 1].first;

      // Cast to Relation first & then generate random payload
      auto object            = std::make_unique<Relation>();
      object->payload.length = payload_sz;
      RandomGenerator::GetRandRepetitiveString(reinterpret_cast<uint8_t *>(object->payload.data), 100UL, payload_sz);

      relation.Insert({key}, *object);
      inrow_count++;
    }
  }

  void ExecuteTxnNoBlobRepresent() {
    Ensure(!enable_blob_rep);

    auto access_key = DetermineSearchKey();
    auto found      = relation.LookUp({access_key}, [&](const Relation &rec) {
      std::unique_ptr<uint8_t[], FreeDelete> payload = nullptr;
      payload.reset(reinterpret_cast<uint8_t *>(malloc(rec.PayloadSize())));
      std::memcpy(payload.get(), rec.payload.data, rec.PayloadSize());
    });
    Ensure(found);
  }

  // -------------------------------------------------------------------------------------
  void LoadInitialData(const tbb::blocked_range<UInteger> &range) {
    u8 tmp[Relation::MAX_RECORD_SIZE];
    Relation *record;

    for (auto key = range.begin(); key < range.end(); key++) {
      // Generate key-value on aligned memory block
      auto payload_sz = characteristic[key - 1].first;
      std::unique_ptr<uint8_t[], FreeDelete> payload(reinterpret_cast<uint8_t *>(malloc(payload_sz)));
      RandomGenerator::GetRandRepetitiveString(payload.get(), 100UL, payload_sz);

      if (payload_sz > MAX_INROW_SIZE) {
        auto blob_rep = relation.RegisterBlob({payload.get(), payload_sz}, {}, false);
        record        = new (tmp) Relation(0, blob_rep);
        offrow_count++;
      } else {
        record = new (tmp) Relation(payload_sz, {payload.get(), payload_sz});
        inrow_count++;
      }

      relation.Insert({key}, *record);
    }
  }

  void ExecuteReadOnlyTxn() {
    Ensure(enable_blob_rep);

    auto access_key = DetermineSearchKey();
    uint8_t tuple[MAX_INROW_SIZE];
    bool is_offrow = false;
    auto found     = relation.LookUp({access_key}, [&](const Relation &rec) {
      is_offrow = rec.IsOffrowRecord();
      std::memcpy(tuple, const_cast<Relation &>(rec).payload, rec.PayloadSize());
    });
    Ensure(found);
    if (is_offrow) {
      relation.LoadBlob(
        tuple,
        [&](std::span<const u8> blob_data) {
          std::unique_ptr<uint8_t[], FreeDelete> payload = nullptr;
          payload.reset(reinterpret_cast<uint8_t *>(malloc(blob_data.size())));
          std::memcpy(payload.get(), blob_data.data(), blob_data.size());
        },
        false);
    }
  }
};

}  // namespace wiki