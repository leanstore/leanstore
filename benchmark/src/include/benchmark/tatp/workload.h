#pragma once

#include "benchmark/adapters/leanstore_adapter.h"
#include "benchmark/tatp/config.h"
#include "benchmark/tatp/schema.h"
#include "benchmark/utils/misc.h"
#include "common/rand.h"

#include "share_headers/config.h"
#include "share_headers/db_types.h"
#include "share_headers/logger.h"
#include "tbb/blocked_range.h"

#include <algorithm>
#include <functional>
#include <span>

namespace tatp {

enum ResultCode { TATP_OK, TATP_MISSING };

/**
 * @brief TATP Benchmark Description: https://tatpbenchmark.sourceforge.net/TATP_Description.pdf
 */
template <template <typename> class AdapterType>
struct TATPWorkload {
  // Env
  const UInteger num_subcribers;  // Number of subcribers
  const Integer constant_a;       // Constant A config in TATP benchmark

  // All relations
  AdapterType<SubscriberType> subcriber;
  AdapterType<SubscriberNbrIndex> subcriber_nbr;
  AdapterType<AccessInfoType> access_info;
  AdapterType<SpecialFacilityType> special_facility;
  AdapterType<CallForwardingType> call_forwarding;

  template <typename... Params>
  explicit TATPWorkload(Integer num_subcribers, Params &&...params)
      : num_subcribers(num_subcribers),
        constant_a(num_subcribers <= 1000000    ? 65535
                   : num_subcribers <= 10000000 ? 1048575
                                                : 2097151),
        subcriber(AdapterType<SubscriberType>(std::forward<Params>(params)...)),
        subcriber_nbr(AdapterType<SubscriberNbrIndex>(std::forward<Params>(params)...)),
        access_info(AdapterType<AccessInfoType>(std::forward<Params>(params)...)),
        special_facility(AdapterType<SpecialFacilityType>(std::forward<Params>(params)...)),
        call_forwarding(AdapterType<CallForwardingType>(std::forward<Params>(params)...)) {}

  auto GenerateSubcriberID() -> UniqueID {
    return (UniformRand(0UL, constant_a) | UniformRand(1UL, num_subcribers)) % num_subcribers + 1UL;
  }

  // -------------------------------------------------------------------------------------
  // Initial data loader
  void LoadSubcribers();
  void LoadSubcriberData(UniqueID s_id);

  // -------------------------------------------------------------------------------------
  // Transaction type
  auto GetSubcriberData(UniqueID s_id) -> ResultCode;
  auto GetNewDestination(UniqueID s_id, uint8_t sf_type, uint8_t start_time, uint8_t end_time) -> ResultCode;
  auto GetAccessData(UniqueID s_id, uint8_t ai_type) -> ResultCode;
  auto UpdateSubcriberData(UniqueID s_id, bool bit_1, uint8_t sf_type, uint8_t data_a) -> ResultCode;
  auto UpdateLocation(const Varchar<15> &sub_nbr, UInteger vlr_location) -> ResultCode;
  auto InsertCallForwarding(const Varchar<15> &sub_nbr, uint8_t sf_type, uint8_t start_time, uint8_t end_time,
                            const Varchar<15> &numberx) -> ResultCode;
  auto DeleteCallForwarding(const Varchar<15> &sub_nbr, uint8_t sf_type, uint8_t start_time) -> ResultCode;

  // -------------------------------------------------------------------------------------
  auto ExecuteTransaction() -> ResultCode;
};

}  // namespace tatp