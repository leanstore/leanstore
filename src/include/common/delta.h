#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <span>

/* Delta log record for UpdateInPlace, i.e. Update for fixed-size records only */
struct FixedSizeDelta {
  struct Slot {
    uint16_t offset;
    uint16_t length;
  };

  uint8_t count = 0;
  uint16_t size = 0;
  Slot slots[];

  static constexpr auto MetadataSize(uint8_t count) -> uint16_t {
    return sizeof(FixedSizeDelta) + (count * sizeof(Slot));
  }

  explicit FixedSizeDelta(uint8_t count) : count(count), size(MetadataSize(count)) {}

  void UpdateDeltaPayload(std::span<uint8_t> tuple_payload) {
    auto off = sizeof(FixedSizeDelta) + (count * sizeof(Slot));
    for (auto idx = 0U; idx < count; idx++) {
      assert((slots[idx].offset + slots[idx].length) <= tuple_payload.size());
      std::memcpy(reinterpret_cast<uint8_t *>(this) + off, tuple_payload.data() + slots[idx].offset, slots[idx].length);
    }
  }
};

// -------------------------------------------------------------------------------------
// Helpers to generate a delta placeholder that describes which attributes are in-place updating in a fixed-size record
#define DELTA_FILL_SLOT(name, index, type, attribute)      \
  (name)->slots[index].offset = offsetof(type, attribute); \
  (name)->slots[index].length = sizeof(type::attribute);   \
  (name)->size += sizeof(type::attribute);

#define DELTA_GENERATOR1(name, type, a0)                                \
  u8 name##_buffer[FixedSizeDelta::MetadataSize(1) + sizeof(type::a0)]; \
  auto name = new (name##_buffer) FixedSizeDelta(1);                    \
  DELTA_FILL_SLOT(name, 0, type, a0);

#define DELTA_GENERATOR2(name, type, a0, a1)                                               \
  u8 name##_buffer[FixedSizeDelta::MetadataSize(2) + sizeof(type::a0) + sizeof(type::a1)]; \
  auto name = new (name##_buffer) FixedSizeDelta(2);                                       \
  DELTA_FILL_SLOT(name, 0, type, a0);                                                      \
  DELTA_FILL_SLOT(name, 1, type, a1);

#define DELTA_GENERATOR3(name, type, a0, a1, a2)                                                              \
  u8 name##_buffer[FixedSizeDelta::MetadataSize(3) + sizeof(type::a0) + sizeof(type::a1) + sizeof(type::a2)]; \
  auto name = new (name##_buffer) FixedSizeDelta(3);                                                          \
  DELTA_FILL_SLOT(name, 0, type, a0);                                                                         \
  DELTA_FILL_SLOT(name, 1, type, a1);                                                                         \
  DELTA_FILL_SLOT(name, 2, type, a2);

#define DELTA_GENERATOR4(name, type, a0, a1, a2, a3)                                                          \
  u8 name##_buffer[FixedSizeDelta::MetadataSize(4) + sizeof(type::a0) + sizeof(type::a1) + sizeof(type::a2) + \
                   sizeof(type::a3)];                                                                         \
  auto name = new (name##_buffer) FixedSizeDelta(4);                                                          \
  DELTA_FILL_SLOT(name, 0, type, a0);                                                                         \
  DELTA_FILL_SLOT(name, 1, type, a1);                                                                         \
  DELTA_FILL_SLOT(name, 2, type, a2);                                                                         \
  DELTA_FILL_SLOT(name, 3, type, a3);
