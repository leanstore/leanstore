#include "benchmark/tpcc/workload.h"
#include "benchmark/adapters/leanstore_adapter.h"

#include <algorithm>
#include <random>

namespace tpcc {

template <template <typename> class AdapterType>
void TPCCWorkload<AdapterType>::QueryCustomerInfo(Integer w_id, Integer d_id,
                                                  std::variant<Integer, Varchar<16>> &c_info, Integer &out_c_id,
                                                  Varchar<16> &out_c_last) {
  // Retrieve customer info
  if (std::holds_alternative<Integer>(c_info)) {
    // query customer by id
    out_c_id   = std::get<Integer>(c_info);
    out_c_last = customer.LookupField({w_id, d_id, out_c_id}, &CustomerType::c_last);
  } else {
    // query customer by last name
    Ensure(std::holds_alternative<Varchar<16>>(c_info));
    out_c_last = std::get<Varchar<16>>(c_info);
    std::vector<Integer> c_ids;
    customer_wdc.Scan({w_id, d_id, out_c_last, {}}, [&](const CustomerWDCType::Key &key, const CustomerWDCType &rec) {
      if (key.c_w_id == w_id && key.c_d_id == d_id && key.c_last == out_c_last) {
        c_ids.push_back(rec.c_id);
        return true;
      }
      return false;
    });
    if (c_ids.empty()) {
      // TODO(Duy): rollback txn
      return;
    }
    out_c_id = c_ids[(c_ids.size() - 1) / 2];
  }
}

template <template <typename> class AdapterType>
void TPCCWorkload<AdapterType>::NewOrder(Integer w_id, Integer d_id, Integer c_id,
                                         const std::vector<Integer> &line_numbers, const std::vector<Integer> &supwares,
                                         const std::vector<Integer> &item_ids, const std::vector<Integer> &quantity_s,
                                         Timestamp timestamp) {
  Numeric w_tax      = warehouse.LookupField({w_id}, &WarehouseType::w_tax);
  Numeric c_discount = customer.LookupField({w_id, d_id, c_id}, &CustomerType::c_discount);
  Numeric d_tax;
  Integer o_id;

  // Update District
  district.UpdateInPlace({w_id, d_id}, [&](DistrictType &rec) {
    d_tax = rec.d_tax;
    o_id  = rec.d_next_o_id++;
  });

  Numeric all_local = 1;
  for (Integer sw : supwares) {
    if (sw != w_id) {
      all_local = 0;
      break;
    }
  }
  Numeric cnt        = line_numbers.size();
  Integer carrier_id = 0; /*null*/

  // Insert into Order & New-Order
  order.Insert({w_id, d_id, o_id}, {c_id, timestamp, carrier_id, cnt, all_local});
  if (enable_order_wdc_index) { order_wdc.Insert({w_id, d_id, c_id, o_id}, OrderWDCType()); }
  neworder.Insert({w_id, d_id, o_id}, NewOrderType());

  // Loop each ongoing order, update the corresponding stock
  for (size_t idx = 0; idx < line_numbers.size(); idx++) {
    Integer qty = quantity_s[idx];
    stock.UpdateInPlace({supwares[idx], item_ids[idx]}, [&](StockType &rec) {
      auto &s_quantity = rec.s_quantity;  // Attention: we also modify s_quantity
      s_quantity       = (s_quantity >= qty + 10) ? s_quantity - qty : s_quantity + 91 - qty;
      rec.s_remote_cnt += static_cast<double>(supwares[idx] != w_id);
      rec.s_order_cnt++;
      rec.s_ytd += qty;
    });
  }

  // Loop each ongoing order, insert the corresponding order line
  for (size_t idx = 0; idx < line_numbers.size(); idx++) {
    Integer line_no = line_numbers[idx];
    Integer supware = supwares[idx];
    Integer itemid  = item_ids[idx];
    Numeric qty     = quantity_s[idx];

    Varchar<24> s_dist;
    Varchar<1> brand_generic("B");

    Numeric i_price = item.LookupField({itemid}, &ItemType::i_price);  // TODO(Duy): rollback on miss
    item.LookUp({itemid}, [&](const ItemType &rec) {
      i_price = rec.i_price;
      if (!rec.i_data.EndsWith("ORIGINAL")) { brand_generic = "G"; }
    });

    stock.LookUp({w_id, itemid}, [&](const StockType &rec) {
      rec.AcquireSDist(d_id, s_dist);
      if (brand_generic != "G" && !rec.s_data.EndsWith("ORIGINAL")) { brand_generic = "G"; }
    });
    Numeric ol_amount       = qty * i_price * (1.0 + w_tax + d_tax) * (1.0 - c_discount);
    Timestamp ol_delivery_d = 0;  // NULL
    orderline.Insert({w_id, d_id, o_id, line_no}, {itemid, supware, ol_delivery_d, qty, ol_amount, s_dist});
  }
}

template <template <typename> class AdapterType>
void TPCCWorkload<AdapterType>::DoNewOrderRandom(Integer w_id) {
  Integer d_id   = UniformRand(1, D_PER_WH);
  Integer c_id   = GetCustomerID();
  Integer ol_cnt = UniformRand(5, 15);

  std::vector<Integer> line_numbers;
  line_numbers.reserve(ol_cnt);
  std::vector<Integer> supwares;
  supwares.reserve(ol_cnt);
  std::vector<Integer> item_ids;
  item_ids.reserve(ol_cnt);
  std::vector<Integer> quantity_s;
  quantity_s.reserve(ol_cnt);

  for (Integer idx = 1; idx <= ol_cnt; idx++) {
    Integer supware = w_id;
    if (enable_cross_warehouses && UniformRand(1, 100) == 1) {
      // ATTN: remote transaction
      supware = UniformRandExcept(1, warehouse_count, w_id);
    }
    Integer itemid = GetItemID();
    line_numbers.push_back(idx);
    supwares.push_back(supware);
    item_ids.push_back(itemid);
    quantity_s.push_back(UniformRand(1, 10));
  }
  NewOrder(w_id, d_id, c_id, line_numbers, supwares, item_ids, quantity_s, CurrentTimestamp());
}

// -------------------------------------------------------------------------------------
template <template <typename> class AdapterType>
void TPCCWorkload<AdapterType>::Delivery(Integer w_id, Integer carrier_id, Timestamp datetime) {
  for (Integer d_id = 1; d_id <= D_PER_WH; d_id++) {
    // Find min order-id from New Order
    Integer o_id = MinInteger;
    neworder.Scan({w_id, d_id, MinInteger}, [&](const NewOrderType::Key &key, const NewOrderType &) {
      if (key.no_w_id == w_id && key.no_d_id == d_id) { o_id = key.no_o_id; }
      return false;
    });
    if (o_id == MinInteger) {  // Should rarely happen
      LOG_DEBUG("WARNING(1): delivery tx skipped for warehouse(%d), district(%d)", w_id, d_id);
      continue;
    }

    // Erase the corresponding row in NewOrder
    const auto ret = neworder.Erase({w_id, d_id, o_id});
    Ensure(ret || manually_handle_isolation_anomalies);

    // Query customber ID & order count of the corresponding order in Order relation
    Integer c_id;
    Integer ol_cnt = MinInteger;
    if (manually_handle_isolation_anomalies) {
      order.Scan({w_id, d_id, o_id}, [&](const OrderType::Key &, const OrderType &rec) {
        ol_cnt = rec.o_ol_cnt;
        c_id   = rec.o_c_id;
        return false;
      });
      if (ol_cnt == MinInteger) {
        LOG_DEBUG("WARNING(2): delivery tx skipped for warehouse(%d), district(%d)", w_id, d_id);
        continue;
      }
    } else {
      order.LookUp({w_id, d_id, o_id}, [&](const OrderType &rec) {
        ol_cnt = rec.o_ol_cnt;
        c_id   = rec.o_c_id;
      });
    }

    // Validate SI
    if (manually_handle_isolation_anomalies) {
      bool is_safe_to_continue = false;
      order.Scan({w_id, d_id, o_id}, [&](const OrderType::Key &key, const OrderType &rec) {
        if (key.o_w_id == w_id && key.o_d_id == d_id && key.o_id == o_id) {
          is_safe_to_continue = true;
          ol_cnt              = rec.o_ol_cnt;
          c_id                = rec.o_c_id;
        } else {
          is_safe_to_continue = false;
        }
        return false;
      });
      if (!is_safe_to_continue) {
        LOG_DEBUG("WARNING(3): Isolation chk failed for warehouse(%d), district(%d)", w_id, d_id);
        continue;
      }
    }

    // Update the carrier info of the processed Order
    order.UpdateInPlace({w_id, d_id, o_id}, [&](OrderType &rec) { rec.o_carrier_id = carrier_id; });

    // Select all Order line of the processed Order
    if (manually_handle_isolation_anomalies) {
      // Check if all orderlines have been inserted
      bool is_safe_to_continue = false;
      orderline.Scan({w_id, d_id, o_id, ol_cnt}, [&](const OrderLineType::Key &key, const OrderLineType &) {
        is_safe_to_continue =
          (key.ol_w_id == w_id && key.ol_d_id == d_id && key.ol_o_id == o_id && key.ol_number == ol_cnt);
        return false;
      });
      if (!is_safe_to_continue) {
        LOG_DEBUG("WARNING(4): Isolation chk failed for warehouse(%d), district(%d)", w_id, d_id);
        continue;
      }
    }

    // Update the OrderLine relation
    Numeric ol_total = 0;
    for (Integer ol_number = 1; ol_number <= ol_cnt; ol_number++) {
      orderline.UpdateInPlace({w_id, d_id, o_id, ol_number}, [&](OrderLineType &rec) {
        ol_total += rec.ol_amount;
        rec.ol_delivery_d = datetime;
      });
    }

    // Update Customer info
    customer.UpdateInPlace({w_id, d_id, c_id}, [&](CustomerType &rec) {
      rec.c_balance += ol_total;
      rec.c_delivery_cnt++;
    });
  }
}

template <template <typename> class AdapterType>
void TPCCWorkload<AdapterType>::DoDeliveryRandom(Integer w_id) {
  Integer carrier_id = UniformRand(1, 10);
  Delivery(w_id, carrier_id, CurrentTimestamp());
}

// -------------------------------------------------------------------------------------
template <template <typename> class AdapterType>
void TPCCWorkload<AdapterType>::StockLevel(Integer w_id, Integer d_id, Integer threshold) {
  /** SELECT d-next-o-id INTO :oid FROM District WHERE d-w-id = :w-id AND d-id = :d-id
   * ;*/
  Integer o_id = district.LookupField({w_id, d_id}, &DistrictType::d_next_o_id);

  /** SELECT COUNT(DISTINCT (s-i-id)) INTO :stock-count FROM Order-Line, Stock
      WHERE ol-w-id = :w-id AND ol-d-id = :did AND ol-o-id < :o-id AND
            ol-o-id >= (:o-id - 20) AND s-w-id = :w-id AND
            s-i-id = ol-i-id AND s-quantity < :threshold ;*/
  std::vector<Integer> items;
  items.reserve(100);
  Integer min_ol_o_id = o_id - 20;
  orderline.Scan({w_id, d_id, min_ol_o_id, MinInteger}, [&](const OrderLineType::Key &key, const OrderLineType &rec) {
    if (key.ol_w_id == w_id && key.ol_d_id == d_id && key.ol_o_id < o_id && key.ol_o_id >= min_ol_o_id) {
      items.push_back(rec.ol_i_id);
      return true;
    }
    return false;
  });
  std::sort(items.begin(), items.end());
  auto last = std::unique(items.begin(), items.end());
  items.erase(last, items.end());
  Numeric count = 0;  // NOLINT
  for (Integer i_id : items) {
    auto res_s_quantity = stock.LookupField({w_id, i_id}, &StockType::s_quantity);
    count += static_cast<Numeric>(res_s_quantity < threshold);
  }
}

template <template <typename> class AdapterType>
void TPCCWorkload<AdapterType>::DoStockLevelRandom(Integer w_id) {
  StockLevel(w_id, UniformRand(1, D_PER_WH), UniformRand(10, 20));
}

// -------------------------------------------------------------------------------------
template <template <typename> class AdapterType>
void TPCCWorkload<AdapterType>::OrderStatus(Integer w_id, Integer d_id, std::variant<Integer, Varchar<16>> c_info) {
  Integer c_id;
  Varchar<16> c_last;

  // Retrieve customer info
  QueryCustomerInfo(w_id, d_id, c_info, c_id, c_last);

  // Select Max(order-id) of the above customer from Order relation
  Integer o_id = -1;
  if (enable_order_wdc_index) {
    order_wdc.ScanDesc({w_id, d_id, c_id, MaxInteger}, [&](const OrderWDCType::Key &key, const OrderWDCType &) {
      assert(key.o_w_id == w_id);
      assert(key.o_d_id == d_id);
      assert(key.o_c_id == c_id);
      o_id = key.o_id;
      return false;
    });
  } else {
    order.ScanDesc({w_id, d_id, MaxInteger}, [&](const OrderType::Key &key, const OrderType &rec) {
      if (key.o_w_id == w_id && key.o_d_id == d_id && rec.o_c_id == c_id) {
        o_id = key.o_id;
        return false;
      }
      return true;
    });
  }
  Ensure(o_id > -1);

  // Retrieve the corresponding Order Line
  Integer ol_i_id;          // NOLINT
  Integer ol_supply_w_id;   // NOLINT
  Timestamp ol_delivery_d;  // NOLINT
  Numeric ol_quantity;      // NOLINT
  Numeric ol_amount;        // NOLINT
  orderline.Scan({w_id, d_id, o_id, MinInteger}, [&](const OrderLineType::Key &key, const OrderLineType &rec) {
    if (key.ol_w_id == w_id && key.ol_d_id == d_id && key.ol_o_id == o_id) {
      ol_i_id        = rec.ol_i_id;
      ol_supply_w_id = rec.ol_supply_w_id;
      ol_delivery_d  = rec.ol_delivery_d;
      ol_quantity    = rec.ol_quantity;
      ol_amount      = rec.ol_amount;
      return true;
    }
    return false;
  });
}

template <template <typename> class AdapterType>
void TPCCWorkload<AdapterType>::DoOrderStatusRandom(Integer w_id) {
  Integer d_id = UniformRand(1, D_PER_WH);
  if (UniformRand(1, 100) <= 40) {
    OrderStatus(w_id, d_id, GetCustomerID());
  } else {
    OrderStatus(w_id, d_id, GenName(GetNonUniformRandomLastNameForRun()));
  }
}

// -------------------------------------------------------------------------------------
template <template <typename> class AdapterType>
void TPCCWorkload<AdapterType>::Payment(Integer w_id, Integer d_id, Integer c_w_id, Integer c_d_id, Timestamp h_date,
                                        Numeric h_amount, Timestamp datetime,
                                        std::variant<Integer, Varchar<16>> c_info) {
  // Query Warehouse info
  Varchar<10> w_name;
  Varchar<20> w_street_1;
  Varchar<20> w_street_2;
  Varchar<20> w_city;
  Varchar<2> w_state;
  Varchar<9> w_zip;
  Numeric w_ytd;  // NOLINT
  warehouse.LookUp({w_id}, [&](const WarehouseType &rec) {
    w_name     = rec.w_name;
    w_street_1 = rec.w_street_1;
    w_street_2 = rec.w_street_2;
    w_city     = rec.w_city;
    w_state    = rec.w_state;
    w_zip      = rec.w_zip;
    w_ytd      = rec.w_ytd;
  });

  // Query District info
  Varchar<10> d_name;
  Varchar<20> d_street_1;
  Varchar<20> d_street_2;
  Varchar<20> d_city;
  Varchar<2> d_state;
  Varchar<9> d_zip;
  Numeric d_ytd;  // NOLINT
  district.LookUp({w_id, d_id}, [&](const DistrictType &rec) {
    d_name     = rec.d_name;
    d_street_1 = rec.d_street_1;
    d_street_2 = rec.d_street_2;
    d_city     = rec.d_city;
    d_state    = rec.d_state;
    d_zip      = rec.d_zip;
    d_ytd      = rec.d_ytd;
  });

  // Query customer info
  Integer c_id;
  Varchar<16> c_last;
  QueryCustomerInfo(w_id, d_id, c_info, c_id, c_last);

  // Update Warehouse's balance & District's balance
  warehouse.UpdateInPlace({w_id}, [&](WarehouseType &rec) { rec.w_ytd += h_amount; });
  district.UpdateInPlace({w_id, d_id}, [&](DistrictType &rec) { rec.d_ytd += h_amount; });

  // Customer Credit Information
  Varchar<500> c_data;
  Varchar<2> c_credit;
  Numeric c_balance;
  Numeric c_ytd_payment;
  Numeric c_payment_cnt;
  customer.LookUp({c_w_id, c_d_id, c_id}, [&](const CustomerType &rec) {
    c_data        = rec.c_data;
    c_credit      = rec.c_credit;
    c_balance     = rec.c_balance;
    c_ytd_payment = rec.c_ytd_payment;
    c_payment_cnt = rec.c_payment_cnt;
  });
  Numeric c_new_balance     = c_balance - h_amount;
  Numeric c_new_ytd_payment = c_ytd_payment + h_amount;
  Numeric c_new_payment_cnt = c_payment_cnt + 1;

  if (c_credit == "BC") {
    // Bad credit
    Varchar<500> c_new_data;
    auto num_chars =
      snprintf(c_new_data.data, 500, "| %4d %2d %4d %2d %4d $%7.2f %lu %s%s %s", c_id, c_d_id, c_w_id, d_id, w_id,
               h_amount, h_date, w_name.ToString().c_str(), d_name.ToString().c_str(), c_data.ToString().c_str());
    c_new_data.length = num_chars;
    if (c_new_data.length > 500) { c_new_data.length = 500; }
    customer.UpdateInPlace({c_w_id, c_d_id, c_id}, [&](CustomerType &rec) {
      rec.c_data        = c_new_data;
      rec.c_balance     = c_new_balance;
      rec.c_ytd_payment = c_new_ytd_payment;
      rec.c_payment_cnt = c_new_payment_cnt;
    });
  } else {
    customer.UpdateInPlace({c_w_id, c_d_id, c_id}, [&](CustomerType &rec) {
      rec.c_data        = "";
      rec.c_balance     = c_new_balance;
      rec.c_ytd_payment = c_new_ytd_payment;
      rec.c_payment_cnt = c_new_payment_cnt;
    });
  }

  // Insert payment history
  Varchar<24> h_new_data = Varchar<24>(w_name) || Varchar<24>("    ") || d_name;
  auto t_id              = static_cast<Integer>(tpcc_thread_id);
  Integer h_id           = history_pk_counter++;
  history.Insert({t_id, h_id}, {c_id, c_d_id, c_w_id, d_id, w_id, datetime, h_amount, h_new_data});
}

template <template <typename> class AdapterType>
void TPCCWorkload<AdapterType>::DoPaymentRandom(Integer w_id) {
  Integer d_id   = UniformRand(1, D_PER_WH);
  Integer c_w_id = w_id;
  Integer c_d_id = d_id;
  if (enable_cross_warehouses && UniformRand(1, 100) > 85) {
    // ATTN: cross warehouses transaction
    c_w_id = UniformRandExcept(1, warehouse_count, w_id);
    c_d_id = UniformRand(1, D_PER_WH);
  }
  Numeric h_amount = RandomNumeric(1.00, 5000.00);
  Timestamp h_date = CurrentTimestamp();

  if (UniformRand(1, 100) <= 60) {
    Payment(w_id, d_id, c_w_id, c_d_id, h_date, h_amount, CurrentTimestamp(),
            GenName(GetNonUniformRandomLastNameForRun()));
  } else {
    Payment(w_id, d_id, c_w_id, c_d_id, h_date, h_amount, CurrentTimestamp(), GetCustomerID());
  }
}

// -------------------------------------------------------------------------------------
// Initial data loader

template <template <typename> class AdapterType>
void TPCCWorkload<AdapterType>::LoadStock(Integer w_id) {
  for (Integer idx = 0; idx < ITEMS_CNT; idx++) {
    Varchar<50> s_data = RandomString<50>(25, 50);
    if (Rand(10) == 0) {
      // 10% of stock data should be "ORIGINAL"
      s_data.length = Rand(s_data.length - 8);
      s_data        = s_data || Varchar<8>("ORIGINAL");
    }
    stock.Insert({w_id, idx + 1}, {RandomNumeric(10, 100), RandomString<24>(24, 24), RandomString<24>(24, 24),
                                   RandomString<24>(24, 24), RandomString<24>(24, 24), RandomString<24>(24, 24),
                                   RandomString<24>(24, 24), RandomString<24>(24, 24), RandomString<24>(24, 24),
                                   RandomString<24>(24, 24), RandomString<24>(24, 24), 0, 0, 0, s_data});
  }
}

template <template <typename> class AdapterType>
void TPCCWorkload<AdapterType>::LoadDistrinct(Integer w_id) {
  for (Integer idx = 1; idx <= D_PER_WH; idx++) {
    district.Insert({w_id, idx}, {RandomString<10>(6, 10), RandomString<20>(10, 20), RandomString<20>(10, 20),
                                  RandomString<20>(10, 20), RandomString<2>(2, 2), RandomZip(),
                                  RandomNumeric(0.0000, 0.2000), 3000000, 3001});
  }
}

template <template <typename> class AdapterType>
void TPCCWorkload<AdapterType>::LoadCustomer(Integer w_id, Integer d_id) {
  Timestamp now = CurrentTimestamp();
  for (Integer idx = 0; idx < C_PER_D; idx++) {
    auto c_last   = (idx < 1000) ? GenName(idx) : GenName(GetNonUniformRandomLastNameForLoad());
    auto c_first  = RandomString<16>(8, 16);
    auto c_credit = Varchar<2>(Rand(10) != 0 ? "GC" : "BC");
    customer.Insert({w_id, d_id, idx + 1},
                    {c_first, "OE", c_last, RandomString<20>(10, 20), RandomString<20>(10, 20),
                     RandomString<20>(10, 20), RandomString<2>(2, 2), RandomZip(), RandomNumberString<16>(), now,
                     c_credit, 50000.00, RandomNumeric(0.0000, 0.5000), -10.00, 1, 0, 0, RandomString<500>(300, 500)});
    customer_wdc.Insert({w_id, d_id, c_last, c_first}, {idx + 1});
    auto t_id    = static_cast<Integer>(tpcc_thread_id);
    Integer h_id = history_pk_counter++;
    history.Insert({t_id, h_id}, {idx + 1, d_id, w_id, d_id, w_id, now, 10.00, RandomString<24>(12, 24)});
  }
}

template <template <typename> class AdapterType>
void TPCCWorkload<AdapterType>::LoadOrder(Integer w_id, Integer d_id) {
  // Generate 3000 orders
  //  first 2100 orders are existing ones, i.e. in Order relation
  //  the rest are new, i.e. inserted into NewOrder relation
  Timestamp now = CurrentTimestamp();
  std::vector<Integer> c_ids;
  for (Integer i = 1; i <= C_PER_D; i++) { c_ids.push_back(i); }
  std::shuffle(c_ids.begin(), c_ids.end(), std::random_device());
  Integer o_id = 1;
  for (Integer o_c_id : c_ids) {
    Integer o_carrier_id = (o_id < 2101) ? UniformRand(1, MAX_CARRIER_ID) : 0;
    Numeric o_ol_cnt     = Rand(10) + 5;

    order.Insert({w_id, d_id, o_id}, {o_c_id, now, o_carrier_id, o_ol_cnt, 1});
    if (enable_order_wdc_index) { order_wdc.Insert({w_id, d_id, o_c_id, o_id}, OrderWDCType()); }

    for (Integer ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
      Timestamp ol_delivery_d = 0;
      if (o_id < 2101) { ol_delivery_d = now; }
      Numeric ol_amount     = (o_id < 2101) ? 0 : RandomNumeric(0.01, 9999.99);
      const Integer ol_i_id = Rand(ITEMS_CNT) + 1;
      orderline.Insert({w_id, d_id, o_id, ol_number},
                       {ol_i_id, w_id, ol_delivery_d, 5, ol_amount, RandomString<24>(24, 24)});
    }
    o_id++;
  }

  for (Integer i = 2100; i <= 3000; i++) { neworder.Insert({w_id, d_id, i}, NewOrderType()); }
}

template <template <typename> class AdapterType>
void TPCCWorkload<AdapterType>::LoadItem() {
  for (Integer idx = 1; idx <= ITEMS_CNT; idx++) {
    auto i_data = RandomString<50>(25, 50);
    if (Rand(10) == 0) {
      i_data.length = Rand(i_data.length - 8);
      i_data        = i_data || Varchar<8>("ORIGINAL");
    }
    item.Insert({idx}, {UniformRand(1, 10000), RandomString<24>(14, 24), RandomNumeric(1.00, 100.00), i_data});
  }
}

template <template <typename> class AdapterType>
void TPCCWorkload<AdapterType>::LoadWarehouse() {
  for (Integer idx = 1; idx <= warehouse_count; idx++) {
    warehouse.Insert(
      {idx}, {RandomString<10>(6, 10), RandomString<20>(10, 20), RandomString<20>(10, 20), RandomString<20>(10, 20),
              RandomString<2>(2, 2), RandomZip(), RandomNumeric(0.1000, 0.2000), 3000000});
  }
}

// -------------------------------------------------------------------------------------
template <template <typename> class AdapterType>
void TPCCWorkload<AdapterType>::InitializeThread() {
  if (tpcc_thread_id > 0) { return; }
  tpcc_thread_id = tpcc_thread_id_counter++;
}

template <template <typename> class AdapterType>
auto TPCCWorkload<AdapterType>::ExecuteTransaction(Integer w_id) -> int {
  auto rnd = UniformRand(1, 100);
  if (rnd <= 43) {
    DoPaymentRandom(w_id);
    return 0;
  }
  rnd -= 43;
  if (rnd <= 4) {
    DoOrderStatusRandom(w_id);
    return 1;
  }
  rnd -= 4;
  if (rnd <= 4) {
    DoDeliveryRandom(w_id);
    return 2;
  }
  rnd -= 4;
  if (rnd <= 4) {
    DoStockLevelRandom(w_id);
    return 3;
  }
  Ensure(rnd - 4 <= 45);
  DoNewOrderRandom(w_id);
  return 4;
}

template struct TPCCWorkload<LeanStoreAdapter>;

}  // namespace tpcc