#pragma once

#include "benchmark/tpcc/schema.h"
#include "benchmark/utils/rand.h"
#include "leanstore/leanstore.h"

#include "share_headers/db_types.h"
#include "share_headers/logger.h"

#include <algorithm>
#include <array>
#include <variant>
#include <vector>

namespace tpcc {

template <template <typename> class AdapterType>
struct TPCCWorkload {
  // Default constraints for TPC-C
  static constexpr Integer OL_I_ID_C = 7911;  // in range [0, 8191]
  static constexpr Integer C_ID_C    = 259;   // in range [0, 1023]
  // NOTE: TPC-C 2.1.6.1 specifies that abs(C_LAST_LOAD_C - C_LAST_RUN_C) must
  // be within [65, 119]
  static constexpr Integer C_LAST_LOAD_C = 157;  // in range [0, 255]
  static constexpr Integer C_LAST_RUN_C  = 223;  // in range [0, 255]

  // TPC-C run-time constraints
  static constexpr Integer C_PER_D        = 3000;    // Each district has 3k customers
  static constexpr Integer D_PER_WH       = 10;      // Each warehouse has 10 districts
  static constexpr Integer ITEMS_CNT      = 100000;  // Maximum of 100K items
  static constexpr Integer MAX_CARRIER_ID = 10;      // Maximum carrier ID

  // TPC-C name generator
  static constexpr std::array NAME_PARTS = {"Bar", "OUGHT", "ABLE",  "PRI",   "PRES",
                                            "ESE", "ANTI",  "CALLY", "ATION", "EING"};

  // All relations
  AdapterType<WarehouseType> warehouse;
  AdapterType<DistrictType> district;
  AdapterType<CustomerType> customer;
  AdapterType<CustomerWDCType> customer_wdc;
  AdapterType<HistoryType> history;
  AdapterType<NewOrderType> neworder;
  AdapterType<OrderType> order;
  AdapterType<OrderWDCType> order_wdc;
  AdapterType<OrderLineType> orderline;
  AdapterType<ItemType> item;
  AdapterType<StockType> stock;

  // Run-time settings for TPC-C
  const Integer warehouse_count;
  // whether we enable index on Order(warehouse-id, district-id, customer-id, order-id)
  const bool enable_order_wdc_index = true;
  // whether we enable transaction on remote warehouse
  const bool enable_cross_warehouses = true;
  // handle isolation anomalies manually (mostly for LeanStore)
  //  a hack because of the missing transaction and concurrency control
  const bool manually_handle_isolation_anomalies = true;

  // Run-time TPC-C counters
  inline static thread_local Integer history_pk_counter     = 0;
  inline static thread_local Integer tpcc_thread_id         = 0;
  inline static std::atomic<Integer> tpcc_thread_id_counter = 1;

  // Random utilities
  auto GenName(Integer id) -> Varchar<16> {
    return Varchar<16>(NAME_PARTS[(id / 100) % 10]) || Varchar<16>(NAME_PARTS[(id / 10) % 10]) ||
           Varchar<16>(NAME_PARTS[id % 10]);
  }

  auto RandomZip() -> Varchar<9> {
    Integer id = Rand(10000);
    Varchar<9> result;
    result.Append(48 + (id / 1000));
    result.Append(48 + (id / 100) % 10);
    result.Append(48 + (id / 10) % 10);
    result.Append(48 + (id % 10));
    return result || Varchar<9>("11111");
  }

  inline auto GetItemID() -> Integer {
    // OL_I_ID_C
    return NonUniformRand(8191, 1, ITEMS_CNT, OL_I_ID_C);
  }

  inline auto GetCustomerID() -> Integer {
    // C_ID_C
    return NonUniformRand(1023, 1, C_PER_D, C_ID_C);
  }

  inline auto GetNonUniformRandomLastNameForRun() -> Integer {
    // C_LAST_RUN_C
    return NonUniformRand(255, 0, 999, C_LAST_RUN_C);
  }

  inline auto GetNonUniformRandomLastNameForLoad() -> Integer {
    // C_LAST_LOAD_C
    return NonUniformRand(255, 0, 999, C_LAST_LOAD_C);
  }

  inline auto CurrentTimestamp() -> Timestamp { return 1; }

  // Query c_id from either c_id (check if customer exists) or by c_last (c's last name)
  void QueryCustomerInfo(Integer w_id, Integer d_id, std::variant<Integer, Varchar<16>> &c_info, Integer &out_c_id,
                         Varchar<16> &out_c_last);

  // -------------------------------------------------------------------------------------
  // Constructor
  template <typename... Params>
  TPCCWorkload(Integer warehouse_count, bool enable_order_wdc_index, bool enable_cross_warehouses,
               bool manually_handle_isolation_anomalies, Params &&...params)
      : warehouse(AdapterType<WarehouseType>(std::forward<Params>(params)...)),
        district(AdapterType<DistrictType>(std::forward<Params>(params)...)),
        customer(AdapterType<CustomerType>(std::forward<Params>(params)...)),
        customer_wdc(AdapterType<CustomerWDCType>(std::forward<Params>(params)...)),
        history(AdapterType<HistoryType>(std::forward<Params>(params)...)),
        neworder(AdapterType<NewOrderType>(std::forward<Params>(params)...)),
        order(AdapterType<OrderType>(std::forward<Params>(params)...)),
        order_wdc(AdapterType<OrderWDCType>(std::forward<Params>(params)...)),
        orderline(AdapterType<OrderLineType>(std::forward<Params>(params)...)),
        item(AdapterType<ItemType>(std::forward<Params>(params)...)),
        stock(AdapterType<StockType>(std::forward<Params>(params)...)),
        warehouse_count(warehouse_count),
        enable_order_wdc_index(enable_order_wdc_index),
        enable_cross_warehouses(enable_cross_warehouses),
        manually_handle_isolation_anomalies(manually_handle_isolation_anomalies) {}

  // -------------------------------------------------------------------------------------
  // Workload operation
  void NewOrder(Integer w_id, Integer d_id, Integer c_id, const std::vector<Integer> &line_numbers,
                const std::vector<Integer> &supwares, const std::vector<Integer> &item_ids,
                const std::vector<Integer> &quantity_s, Timestamp timestamp);
  void DoNewOrderRandom(Integer w_id);
  // -------------------------------------------------------------------------------------
  void Delivery(Integer w_id, Integer carrier_id, Timestamp datetime);
  void DoDeliveryRandom(Integer w_id);
  // -------------------------------------------------------------------------------------
  void StockLevel(Integer w_id, Integer d_id, Integer threshold);
  void DoStockLevelRandom(Integer w_id);
  // -------------------------------------------------------------------------------------
  void OrderStatus(Integer w_id, Integer d_id, std::variant<Integer, Varchar<16>> c_info);
  void DoOrderStatusRandom(Integer w_id);
  // -------------------------------------------------------------------------------------
  void Payment(Integer w_id, Integer d_id, Integer c_w_id, Integer c_d_id, Timestamp h_date, Numeric h_amount,
               Timestamp datetime, std::variant<Integer, Varchar<16>> c_info);
  void DoPaymentRandom(Integer w_id);

  // -------------------------------------------------------------------------------------
  // Initial data loader
  void LoadStock(Integer w_id);
  void LoadDistrinct(Integer w_id);
  void LoadCustomer(Integer w_id, Integer d_id);
  void LoadOrder(Integer w_id, Integer d_id);
  void LoadItem();
  void LoadWarehouse();

  // -------------------------------------------------------------------------------------
  void InitializeThread();
  auto ExecuteTransaction(Integer w_id) -> int;
};

}  // namespace tpcc