#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <span>
#include <cstdio>
#include <format>

using UInteger                       = uint32_t;
using Integer                        = int32_t;
using Timestamp                      = int64_t;
using UniqueID                       = uint64_t;
using Numeric                        = double;
static constexpr Integer MinUInteger = std::numeric_limits<UInteger>::min();
static constexpr Integer MinInteger  = std::numeric_limits<Integer>::min();
static constexpr Integer MaxUInteger = std::numeric_limits<UInteger>::max();
static constexpr Integer MaxInteger  = std::numeric_limits<Integer>::max();

template <uint64_t Size>
struct BytesPayload {
  uint8_t value[Size];

  BytesPayload() = default;

  BytesPayload(const uint8_t *str, uint32_t len) {
    assert(len <= Size);
    std::memcpy(value, str, len);
  }

  BytesPayload(const BytesPayload &other) { std::memcpy(value, other.value, sizeof(value)); }

  auto operator==(BytesPayload &other) -> bool { return (std::memcmp(value, other.value, sizeof(value)) == 0); }

  auto operator!=(BytesPayload &other) -> bool { return !(operator==(other)); }

  auto operator=(const BytesPayload &other) -> BytesPayload & {
    std::memcpy(value, other.value, sizeof(value));
    return *this;
  }

  auto Data() -> uint8_t* { return &value[0]; }
};

template <int MaxLength>
struct Varchar {
  uint32_t length;
  char data[MaxLength] = {0};  // not '\0' terminated

  Varchar() : length(0) {}

  Varchar(const char *str) {
    int l = strlen(str);
    assert(l <= MaxLength);
    length = l;
    std::memcpy(data, str, l);
  }

  Varchar(std::span<const uint8_t> s) {
    assert(s.size() <= MaxLength);
    length = s.size();
    std::memcpy(data, s.data(), s.size());
  }

  template <int OtherMaxLength>
  Varchar(const Varchar<OtherMaxLength> &other) {
    assert(other.length <= MaxLength);
    length = other.length;
    std::memcpy(data, other.data, length);
  }

  Varchar(UniqueID num, uint32_t len) : length(len) {
    assert(len <= MaxLength);
    auto str = std::format("{:0{}}", num, len);
    std::memcpy(data, str.c_str(), len);
  }

  auto MallocSize() const -> size_t { return sizeof(length) + length; }

  auto ToString() const -> std::string { return std::string(data, length); };

  template <int OtherMaxLength>
  Varchar<MaxLength> operator||(const Varchar<OtherMaxLength> &other) const {
    Varchar<MaxLength> tmp;
    assert((static_cast<int32_t>(length) + other.length) <= MaxLength);
    tmp.length = length + other.length;
    std::memcpy(tmp.data, data, length);
    std::memcpy(tmp.data + length, other.data, other.length);
    return tmp;
  }

  bool operator==(const Varchar<MaxLength> &other) const {
    return (length == other.length) && (std::memcmp(data, other.data, length) == 0);
  }

  bool operator!=(const Varchar<MaxLength> &other) const { return !operator==(other); }

  bool operator<(const Varchar<MaxLength> &other) const {
    int cmp = std::memcmp(data, other.data, (length < other.length) ? length : other.length);
    return (cmp == 0) ? length < other.length : cmp < 0;
  }

  void Append(char x) {
    assert(length < MaxLength);
    data[length++] = x;
  }

  auto EndsWith(const char *suffix) const -> bool {
    assert(MaxLength > strlen(suffix));
    if (strcmp(data + (MaxLength - strlen(suffix)), suffix) == 0) { return true; }
    return false;
  }
};
