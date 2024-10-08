#pragma once

#include "share_headers/db_types.h"
#include "typefold/typefold.h"

#include <cstring>

namespace tatp {

struct SubscriberType {
  static constexpr uint32_t TYPE_ID = 0;

  struct Key {
    UniqueID s_id;
  };

  Varchar<15> sub_nbr;
  BytesPayload<10> bit;
  BytesPayload<10> hex;
  BytesPayload<10> byte2;
  UInteger msc_location;
  UInteger vlr_location;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(SubscriberType); }

  static auto FoldKey(uint8_t *out, const SubscriberType::Key &key) -> uint16_t {
    auto pos = Fold(out, key.s_id);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, SubscriberType::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.s_id);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::s_id); }
};

struct SubscriberNbrIndex {
  static constexpr uint32_t TYPE_ID = 1;

  struct Key {
    Varchar<15> sub_nbr;
  };

  UniqueID s_id;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(SubscriberNbrIndex); }

  static auto FoldKey(uint8_t *out, const SubscriberNbrIndex::Key &key) -> uint16_t {
    auto pos = Fold(out, key.sub_nbr);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, SubscriberNbrIndex::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.sub_nbr);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::sub_nbr); }
};

struct AccessInfoType {
  static constexpr uint32_t TYPE_ID = 2;

  struct Key {
    UniqueID s_id;
    uint8_t ai_type;
  };

  uint8_t data1;
  uint8_t data2;
  Varchar<3> data3;
  Varchar<5> data4;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(AccessInfoType); }

  static auto FoldKey(uint8_t *out, const AccessInfoType::Key &key) -> uint16_t {
    auto pos = Fold(out, key.s_id);
    out[pos] = key.ai_type;
    return pos + 1;
  }

  static auto UnfoldKey(const uint8_t *in, AccessInfoType::Key &key) -> uint16_t {
    auto pos    = Unfold(in, key.s_id);
    key.ai_type = in[pos];
    return pos + 1;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::s_id) + sizeof(Key::ai_type); }
};

struct SpecialFacilityType {
  static constexpr uint32_t TYPE_ID = 3;

  struct Key {
    UniqueID s_id;
    uint8_t sf_type;
  };

  bool is_active;
  uint8_t error_cntrl;
  uint8_t data_a;
  Varchar<5> data_b;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(SpecialFacilityType); }

  static auto FoldKey(uint8_t *out, const SpecialFacilityType::Key &key) -> uint16_t {
    auto pos = Fold(out, key.s_id);
    out[pos] = key.sf_type;
    return pos + 1;
  }

  static auto UnfoldKey(const uint8_t *in, SpecialFacilityType::Key &key) -> uint16_t {
    auto pos    = Unfold(in, key.s_id);
    key.sf_type = in[pos];
    return pos + 1;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::s_id) + sizeof(Key::sf_type); }
};

struct CallForwardingType {
  static constexpr uint32_t TYPE_ID = 4;

  struct Key {
    UniqueID s_id;
    uint8_t sf_type;
    uint8_t start_time;
  };

  uint8_t end_time;
  Varchar<15> numberx;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(CallForwardingType); }

  static auto FoldKey(uint8_t *out, const CallForwardingType::Key &key) -> uint16_t {
    auto pos     = Fold(out, key.s_id);
    out[pos]     = key.sf_type;
    out[pos + 1] = key.start_time;
    return pos + 2;
  }

  static auto UnfoldKey(const uint8_t *in, CallForwardingType::Key &key) -> uint16_t {
    auto pos       = Unfold(in, key.s_id);
    key.sf_type    = in[pos];
    key.start_time = in[pos + 1];
    return pos + 2;
  }

  static auto MaxFoldLength() -> uint32_t {
    return 0 + sizeof(Key::s_id) + sizeof(Key::sf_type) + sizeof(Key::start_time);
  }
};

}  // namespace tatp