#include "benchmark/tatp/workload.h"

#include "fmt/ranges.h"

#include <algorithm>
#include <random>

namespace tatp {

template <template <typename> class AdapterType>
void TATPWorkload<AdapterType>::LoadSubcribers() {
  LOG_DEBUG("Load %u subcribers", num_subcribers);
  for (auto idx = 1UL; idx <= num_subcribers; idx++) {
    auto nbr = Varchar<15>(idx, 15);
    subcriber.Insert(
      {idx}, {nbr, RandomByteArray<10, false, true>(), RandomByteArray<10, 0, 15>(), RandomByteArray<10, 0, 255>(),
              RandomGenerator::GetRand<UInteger>(0, std::numeric_limits<UInteger>::max()),
              RandomGenerator::GetRand<UInteger>(0, std::numeric_limits<UInteger>::max())});
    subcriber_nbr.Insert({nbr}, {idx});
  }
  LOG_DEBUG("Load subcribers successfully");
}

template <template <typename> class AdapterType>
void TATPWorkload<AdapterType>::LoadSubcriberData(UniqueID s_id) {
  // Load access info
  auto ai_types = Sample<uint8_t>(UniformRand(1, 4), {1, 2, 3, 4});
  for (auto ai_type : ai_types) {
    access_info.Insert({s_id, ai_type},
                       {RandomGenerator::GetRand<uint8_t>(0, 255), RandomGenerator::GetRand<uint8_t>(0, 255),
                        RandomString<3>(3, 3), RandomString<5>(5, 5)});
  }

  // Load special facility
  auto sf_types = Sample<uint8_t>(UniformRand(1, 4), {1, 2, 3, 4});
  for (auto sf_type : sf_types) {
    special_facility.Insert({s_id, sf_type}, {(UniformRand(0, 99) < 85), RandomGenerator::GetRand<uint8_t>(0, 255),
                                              RandomGenerator::GetRand<uint8_t>(0, 255), RandomString<5>(5, 5)});

    // Load call forwarding
    auto start_times = Sample<uint8_t>(UniformRand(0, 3), {0, 8, 16});
    for (auto start_time : start_times) {
      uint8_t end_time = start_time + RandomGenerator::GetRand<uint8_t>(1, 9);
      call_forwarding.Insert({s_id, sf_type, start_time}, {end_time, RandomNumberString<15>()});
    }
  }
}

template <template <typename> class AdapterType>
auto TATPWorkload<AdapterType>::GetSubcriberData(UniqueID s_id) -> ResultCode {
  Varchar<15> sub_nbr;
  BytesPayload<10> bit;
  BytesPayload<10> hex;
  BytesPayload<10> byte2;
  UInteger msc_location;
  UInteger vlr_location;

  auto found = subcriber.LookUp({s_id}, [&](const SubscriberType &rec) {
    sub_nbr      = rec.sub_nbr;
    bit          = rec.bit;
    hex          = rec.hex;
    byte2        = rec.byte2;
    msc_location = rec.msc_location;
    vlr_location = rec.vlr_location;
  });
  Ensure(found);
  if (msc_location != 0U && vlr_location != 0U) {}  // Prevent these two from being optimized out
  return ResultCode::TATP_OK;
}

template <template <typename> class AdapterType>
auto TATPWorkload<AdapterType>::GetNewDestination(UniqueID s_id, uint8_t sf_type, uint8_t start_time,
                                                  uint8_t end_time) -> ResultCode {
  bool is_active = false;
  auto found =
    special_facility.LookUp({s_id, sf_type}, [&](const SpecialFacilityType &rec) { is_active = rec.is_active; });
  if (found && is_active) {
    std::vector<Varchar<15>> numberx;
    call_forwarding.ScanDesc({s_id, sf_type, start_time},
                             [&](const CallForwardingType::Key &key, const CallForwardingType &rec) {
                               if (key.s_id == s_id && key.sf_type == sf_type && key.start_time == start_time) {
                                 if (end_time < rec.end_time) { numberx.push_back(rec.numberx); }
                                 return true;
                               }
                               return false;
                             });
    Ensure(numberx.size() <= 3);
    found = !numberx.empty();
  }
  return (found) ? ResultCode::TATP_OK : ResultCode::TATP_MISSING;
}

template <template <typename> class AdapterType>
auto TATPWorkload<AdapterType>::GetAccessData(UniqueID s_id, uint8_t ai_type) -> ResultCode {
  uint8_t data1;
  uint8_t data2;
  Varchar<3> data3;
  Varchar<5> data4;

  auto found = access_info.LookUp({s_id, ai_type}, [&](const AccessInfoType &rec) {
    data1 = rec.data1;
    data2 = rec.data2;
    data3 = rec.data3;
    data4 = rec.data4;
  });
  if (data1 != 0U && data2 != 0U) {}  // Prevent these two from being optimized out
  return (found) ? ResultCode::TATP_OK : ResultCode::TATP_MISSING;
}

template <template <typename> class AdapterType>
auto TATPWorkload<AdapterType>::UpdateSubcriberData(UniqueID s_id, bool bit_1, uint8_t sf_type,
                                                    uint8_t data_a) -> ResultCode {
  subcriber.UpdateInPlace({s_id}, [&](SubscriberType &rec) { rec.bit.Data()[0] = static_cast<uint8_t>(bit_1); });
  auto found = special_facility.UpdateInPlace({s_id, sf_type}, [&](SpecialFacilityType &rec) { rec.data_a = data_a; });

  return (found) ? ResultCode::TATP_OK : ResultCode::TATP_MISSING;
}

template <template <typename> class AdapterType>
auto TATPWorkload<AdapterType>::UpdateLocation(const Varchar<15> &sub_nbr, UInteger vlr_location) -> ResultCode {
  UniqueID s_id;
  auto found = subcriber_nbr.LookUp({sub_nbr}, [&](const SubscriberNbrIndex &rec) { s_id = rec.s_id; });
  Ensure(found);

  found = subcriber.UpdateInPlace({s_id}, [&](SubscriberType &rec) { rec.vlr_location = vlr_location; });
  Ensure(found);

  return ResultCode::TATP_OK;
}

template <template <typename> class AdapterType>
auto TATPWorkload<AdapterType>::InsertCallForwarding(const Varchar<15> &sub_nbr, uint8_t sf_type, uint8_t start_time,
                                                     uint8_t end_time, const Varchar<15> &numberx) -> ResultCode {
  // Find s_id
  UniqueID s_id;
  auto found = subcriber_nbr.LookUp({sub_nbr}, [&](const SubscriberNbrIndex &rec) { s_id = rec.s_id; });
  Ensure(found);

  // Find special facility
  std::vector<uint8_t> sf_results;
  special_facility.Scan({s_id, 0}, [&](const SpecialFacilityType::Key &key, const SpecialFacilityType &) {
    if (key.s_id == s_id) {
      sf_results.push_back(key.sf_type);
      return true;
    }
    return false;
  });

  // Insert call forwarding
  // TODO(XXX): Return ResultCode::TATP_DUPLICATED when {s_id, sf_type} already exists
  call_forwarding.Insert({s_id, sf_type, start_time}, {end_time, numberx});
  return ResultCode::TATP_OK;
}

template <template <typename> class AdapterType>
auto TATPWorkload<AdapterType>::DeleteCallForwarding(const Varchar<15> &sub_nbr, uint8_t sf_type,
                                                     uint8_t start_time) -> ResultCode {
  // Find s_id
  UniqueID s_id;
  auto found = subcriber_nbr.LookUp({sub_nbr}, [&](const SubscriberNbrIndex &rec) { s_id = rec.s_id; });
  Ensure(found);

  // Delete call forwarding
  found = call_forwarding.Erase({s_id, sf_type, start_time});
  return (found) ? ResultCode::TATP_OK : ResultCode::TATP_MISSING;
}

template <template <typename> class AdapterType>
auto TATPWorkload<AdapterType>::ExecuteTransaction() -> ResultCode {
  auto s_id = GenerateSubcriberID();
  auto rnd  = UniformRand(1, 100);

  if (rnd <= 35) { return GetSubcriberData(s_id); }
  rnd -= 35;
  if (rnd <= 10) {
    uint8_t sf_type    = UniformRand(1, 4);
    uint8_t start_time = UniformRand(0, 2) * 8;
    uint8_t end_time   = UniformRand(1, 24);
    return GetNewDestination(s_id, sf_type, start_time, end_time);
  }
  rnd -= 10;
  if (rnd <= 35) {
    uint8_t ai_type = UniformRand(1, 4);
    return GetAccessData(s_id, ai_type);
  }
  rnd -= 35;
  if (rnd <= 2) {
    bool bit_1      = RandBool();
    uint8_t sf_type = UniformRand(1, 4);
    uint8_t data_a  = UniformRand(0, 255);
    return UpdateSubcriberData(s_id, bit_1, sf_type, data_a);
  }
  rnd -= 2;
  if (rnd <= 14) {
    auto nbr          = Varchar<15>(s_id, 15);
    auto vlr_location = RandomGenerator::GetRand<UInteger>(0, std::numeric_limits<UInteger>::max());
    return UpdateLocation(nbr, vlr_location);
  }
  switch (rnd - 14) {
    case 1:
    case 2: {
      auto nbr           = Varchar<15>(s_id, 15);
      uint8_t sf_type    = UniformRand(1, 4);
      uint8_t start_time = UniformRand(0, 2) * 8;
      uint8_t end_time   = UniformRand(1, 24);
      auto numberx       = RandomNumberString<15>();
      return InsertCallForwarding(nbr, sf_type, start_time, end_time, numberx);
    }
    case 3:
    case 4: {
      auto nbr           = Varchar<15>(s_id, 15);
      uint8_t sf_type    = UniformRand(1, 4);
      uint8_t start_time = UniformRand(0, 2) * 8;
      return DeleteCallForwarding(nbr, sf_type, start_time);
    }
    default: UnreachableCode();
  }
  return ResultCode::TATP_OK;  // Just to silent static code analysis
}

template struct TATPWorkload<LeanStoreAdapter>;

}  // namespace tatp