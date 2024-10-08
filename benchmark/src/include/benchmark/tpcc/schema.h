#pragma once

#include "common/delta.h"
#include "share_headers/db_types.h"
#include "typefold/typefold.h"

#include <stdexcept>

namespace tpcc {

struct WarehouseType {
  static constexpr uint32_t TYPE_ID = 0;

  struct Key {
    Integer w_id;
  };

  Varchar<10> w_name;
  Varchar<20> w_street_1;
  Varchar<20> w_street_2;
  Varchar<20> w_city;
  Varchar<2> w_state;
  Varchar<9> w_zip;
  Numeric w_tax;
  Numeric w_ytd;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(WarehouseType); }

  static auto FoldKey(uint8_t *out, const WarehouseType::Key &key) -> uint16_t {
    auto pos = Fold(out, key.w_id);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, WarehouseType::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.w_id);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::w_id); }
};

struct DistrictType {
  static constexpr uint32_t TYPE_ID = 1;

  struct Key {
    Integer d_w_id;
    Integer d_id;
  };

  Varchar<10> d_name;
  Varchar<20> d_street_1;
  Varchar<20> d_street_2;
  Varchar<20> d_city;
  Varchar<2> d_state;
  Varchar<9> d_zip;
  Numeric d_tax;
  Numeric d_ytd;
  Integer d_next_o_id;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(DistrictType); }

  static auto FoldKey(uint8_t *out, const DistrictType::Key &key) -> uint16_t {
    auto pos = Fold(out, key.d_w_id);
    pos += Fold(out + pos, key.d_id);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, DistrictType::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.d_w_id);
    pos += Unfold(in + pos, key.d_id);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::d_w_id) + sizeof(Key::d_id); }
};

struct CustomerType {
  static constexpr uint32_t TYPE_ID = 2;

  struct Key {
    Integer c_w_id;
    Integer c_d_id;
    Integer c_id;
  };

  Varchar<16> c_first;
  Varchar<2> c_middle;
  Varchar<16> c_last;
  Varchar<20> c_street_1;
  Varchar<20> c_street_2;
  Varchar<20> c_city;
  Varchar<2> c_state;
  Varchar<9> c_zip;
  Varchar<16> c_phone;
  Timestamp c_since;
  Varchar<2> c_credit;
  Numeric c_credit_lim;
  Numeric c_discount;
  Numeric c_balance;
  Numeric c_ytd_payment;
  Numeric c_payment_cnt;
  Numeric c_delivery_cnt;
  Varchar<500> c_data;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(CustomerType); }

  static auto FoldKey(uint8_t *out, const CustomerType::Key &key) -> uint16_t {
    auto pos = Fold(out, key.c_w_id);
    pos += Fold(out + pos, key.c_d_id);
    pos += Fold(out + pos, key.c_id);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, CustomerType::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.c_w_id);
    pos += Unfold(in + pos, key.c_d_id);
    pos += Unfold(in + pos, key.c_id);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::c_w_id) + sizeof(Key::c_d_id) + sizeof(Key::c_id); }
};

struct CustomerWDCType {
  static constexpr uint32_t TYPE_ID = 3;

  struct Key {
    Integer c_w_id;
    Integer c_d_id;
    Varchar<16> c_last;
    Varchar<16> c_first;
  };

  Integer c_id;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(CustomerWDCType); }

  static auto FoldKey(uint8_t *out, const CustomerWDCType::Key &key) -> uint16_t {
    auto pos = Fold(out, key.c_w_id);
    pos += Fold(out + pos, key.c_d_id);
    pos += Fold(out + pos, key.c_last);
    pos += Fold(out + pos, key.c_first);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, CustomerWDCType::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.c_w_id);
    pos += Unfold(in + pos, key.c_d_id);
    pos += Unfold(in + pos, key.c_last);
    pos += Unfold(in + pos, key.c_first);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t {
    return 0 + sizeof(Key::c_w_id) + sizeof(Key::c_d_id) + sizeof(Key::c_last) + sizeof(Key::c_first);
  }
};

struct HistoryType {
  static constexpr uint32_t TYPE_ID = 4;

  struct Key {
    Integer h_t_id;
    Integer h_pk;
  };

  Integer h_c_id;
  Integer h_c_d_id;
  Integer h_c_w_id;
  Integer h_d_id;
  Integer h_w_id;
  Timestamp h_date;
  Numeric h_amount;
  Varchar<24> h_data;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(HistoryType); }

  static auto FoldKey(uint8_t *out, const HistoryType::Key &key) -> uint16_t {
    auto pos = Fold(out, key.h_t_id);
    pos += Fold(out + pos, key.h_pk);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, HistoryType::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.h_t_id);
    pos += Unfold(in + pos, key.h_pk);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::h_t_id) + sizeof(Key::h_pk); }
};

struct NewOrderType {
  static constexpr uint32_t TYPE_ID = 5;

  struct Key {
    Integer no_w_id;
    Integer no_d_id;
    Integer no_o_id;
  };

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(NewOrderType); }

  static auto FoldKey(uint8_t *out, const NewOrderType::Key &key) -> uint16_t {
    auto pos = Fold(out, key.no_w_id);
    pos += Fold(out + pos, key.no_d_id);
    pos += Fold(out + pos, key.no_o_id);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, NewOrderType::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.no_w_id);
    pos += Unfold(in + pos, key.no_d_id);
    pos += Unfold(in + pos, key.no_o_id);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t {
    return 0 + sizeof(Key::no_w_id) + sizeof(Key::no_d_id) + sizeof(Key::no_o_id);
  }
};

struct OrderType {
  static constexpr uint32_t TYPE_ID = 6;

  struct Key {
    Integer o_w_id;
    Integer o_d_id;
    Integer o_id;
  };

  Integer o_c_id;
  Timestamp o_entry_d;
  Integer o_carrier_id;
  Numeric o_ol_cnt;
  Numeric o_all_local;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(OrderType); }

  static auto FoldKey(uint8_t *out, const OrderType::Key &key) -> uint16_t {
    auto pos = Fold(out, key.o_w_id);
    pos += Fold(out + pos, key.o_d_id);
    pos += Fold(out + pos, key.o_id);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, OrderType::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.o_w_id);
    pos += Unfold(in + pos, key.o_d_id);
    pos += Unfold(in + pos, key.o_id);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::o_w_id) + sizeof(Key::o_d_id) + sizeof(Key::o_id); }
};

struct OrderWDCType {
  static constexpr uint32_t TYPE_ID = 7;

  struct Key {
    Integer o_w_id;
    Integer o_d_id;
    Integer o_c_id;
    Integer o_id;
  };

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(OrderWDCType); }

  static auto FoldKey(uint8_t *out, const OrderWDCType::Key &key) -> uint16_t {
    auto pos = Fold(out, key.o_w_id);
    pos += Fold(out + pos, key.o_d_id);
    pos += Fold(out + pos, key.o_c_id);
    pos += Fold(out + pos, key.o_id);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, OrderWDCType::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.o_w_id);
    pos += Unfold(in + pos, key.o_d_id);
    pos += Unfold(in + pos, key.o_c_id);
    pos += Unfold(in + pos, key.o_id);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t {
    return 0 + sizeof(Key::o_w_id) + sizeof(Key::o_d_id) + sizeof(Key::o_c_id) + sizeof(Key::o_id);
  }
};

struct OrderLineType {
  static constexpr uint32_t TYPE_ID = 8;

  struct Key {
    Integer ol_w_id;
    Integer ol_d_id;
    Integer ol_o_id;
    Integer ol_number;
  };

  Integer ol_i_id;
  Integer ol_supply_w_id;
  Timestamp ol_delivery_d;
  Numeric ol_quantity;
  Numeric ol_amount;
  Varchar<24> ol_dist_info;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(OrderLineType); }

  static auto FoldKey(uint8_t *out, const OrderLineType::Key &key) -> uint16_t {
    auto pos = Fold(out, key.ol_w_id);
    pos += Fold(out + pos, key.ol_d_id);
    pos += Fold(out + pos, key.ol_o_id);
    pos += Fold(out + pos, key.ol_number);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, OrderLineType::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.ol_w_id);
    pos += Unfold(in + pos, key.ol_d_id);
    pos += Unfold(in + pos, key.ol_o_id);
    pos += Unfold(in + pos, key.ol_number);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t {
    return 0 + sizeof(Key::ol_w_id) + sizeof(Key::ol_d_id) + sizeof(Key::ol_o_id) + sizeof(Key::ol_number);
  }
};

struct ItemType {
  static constexpr uint32_t TYPE_ID = 9;

  struct Key {
    Integer i_id;
  };

  Integer i_im_id;
  Varchar<24> i_name;
  Numeric i_price;
  Varchar<50> i_data;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(ItemType); }

  static auto FoldKey(uint8_t *out, const ItemType::Key &key) -> uint16_t {
    auto pos = Fold(out, key.i_id);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, ItemType::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.i_id);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::i_id); }
};

struct StockType {
  static constexpr uint32_t TYPE_ID = 10;

  struct Key {
    Integer s_w_id;
    Integer s_i_id;
  };

  Numeric s_quantity;
  Varchar<24> s_dist_01;
  Varchar<24> s_dist_02;
  Varchar<24> s_dist_03;
  Varchar<24> s_dist_04;
  Varchar<24> s_dist_05;
  Varchar<24> s_dist_06;
  Varchar<24> s_dist_07;
  Varchar<24> s_dist_08;
  Varchar<24> s_dist_09;
  Varchar<24> s_dist_10;
  Numeric s_ytd;
  Numeric s_order_cnt;
  Numeric s_remote_cnt;
  Varchar<50> s_data;

  void AcquireSDist(Integer d_id, Varchar<24> &s_dist) const {
    switch (d_id) {
      case 1: s_dist = s_dist_01; break;
      case 2: s_dist = s_dist_02; break;
      case 3: s_dist = s_dist_03; break;
      case 4: s_dist = s_dist_04; break;
      case 5: s_dist = s_dist_05; break;
      case 6: s_dist = s_dist_06; break;
      case 7: s_dist = s_dist_07; break;
      case 8: s_dist = s_dist_08; break;
      case 9: s_dist = s_dist_09; break;
      case 10: s_dist = s_dist_10; break;
      default: throw std::runtime_error("Unreachable");
    }
  }

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(StockType); }

  static auto FoldKey(uint8_t *out, const StockType::Key &key) -> uint16_t {
    auto pos = Fold(out, key.s_w_id);
    pos += Fold(out + pos, key.s_i_id);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, StockType::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.s_w_id);
    pos += Unfold(in + pos, key.s_i_id);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::s_w_id) + sizeof(Key::s_i_id); }
};

}  // namespace tpcc