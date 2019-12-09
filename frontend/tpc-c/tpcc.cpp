#include "types.hpp"
#include "adapter.hpp"
#include "schema.hpp"
// -------------------------------------------------------------------------------------
#include "/opt/PerfEvent.hpp"
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <cstdint>
#include <string>
#include <vector>
#include <iostream>
#include <algorithm>
#include <unistd.h>
#include <limits>
#include <csignal>
// -------------------------------------------------------------------------------------
DEFINE_uint32(warehouse_count, 1, "");
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore;
// -------------------------------------------------------------------------------------
LeanStoreAdapter<warehouse_t> warehouse;
LeanStoreAdapter<district_t> district;
LeanStoreAdapter<customer_t> customer;
LeanStoreAdapter<customer_wdl_t> customerwdl;
LeanStoreAdapter<history_t> history;
LeanStoreAdapter<neworder_t> neworder;
LeanStoreAdapter<order_t> order;
LeanStoreAdapter<order_wdc_t> order_wdc;
LeanStoreAdapter<orderline_t> orderline;
LeanStoreAdapter<item_t> item;
LeanStoreAdapter<stock_t> stock;
/*
StdMap<warehouse_t> warehouse;
StdMap<district_t> district;
StdMap<customer_t> customer;
StdMap<customer_wdl_t> customerwdl;
StdMap<history_t> history;
StdMap<neworder_t> neworder;
StdMap<order_t> order;
StdMap<order_wdc_t> order_wdc;
StdMap<orderline_t> orderline;
StdMap<item_t> item;
StdMap<stock_t> stock;
 */
// -------------------------------------------------------------------------------------

// load

Integer warehouseCount = FLAGS_warehouse_count;

Integer rnd(Integer n)
{
   return random() % n;
}

Integer randomId(Integer fromId, Integer toId)
{
   return rnd(toId - fromId + 1) + fromId;
}

template<int maxLength>
Varchar<maxLength> randomastring(Integer minLenStr, Integer maxLenStr)
{
   assert(maxLenStr <= maxLength);
   Integer len = rnd(maxLenStr - minLenStr + 1) + minLenStr;
   Varchar<maxLength> result;
   for ( Integer index = 0; index < len; index++ ) {
      Integer i = rnd(62);
      if ( i < 10 )
         result.append(48 + i);
      else if ( i < 36 )
         result.append(64 - 10 + i);
      else
         result.append(96 - 36 + i);
   }
   return result;
}

Varchar<16> randomnstring(Integer minLenStr, Integer maxLenStr)
{
   Integer len = rnd(maxLenStr - minLenStr + 1) + minLenStr;
   Varchar<16> result;
   for ( Integer i = 0; i < len; i++ )
      result.append(48 + rnd(10));
   return result;
}

Varchar<16> namePart(Integer id)
{
   assert(id < 10);
   Varchar<16> data[] = {"Bar", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
   return data[id];
}

Varchar<16> genName(Integer id)
{
   return namePart((id / 100) % 10) || namePart((id / 10) % 10) || namePart(id % 10);
}

Numeric randomNumeric(Numeric min, Numeric max)
{
   double range = (max - min);
   double div = RAND_MAX / range;
   return min + (rand() / div);
}

Varchar<9> randomzip()
{
   Integer id = rnd(10000);
   Varchar<9> result;
   result.append(48 + (id / 1000));
   result.append(48 + (id / 100) % 10);
   result.append(48 + (id / 10) % 10);
   result.append(48 + (id % 10));
   return result || Varchar<9>("11111");
}

Integer nurand(Integer a, Integer x, Integer y)
{
   return (((rnd(a) | rnd((y - x + 1) + x)) + 42) % (y - x + 1)) + x;
}

void loadItem()
{
   for ( Integer i = 1; i <= 100000; i++ ) {
      Varchar<50> i_data = randomastring<50>(25, 50);
      if ( rnd(10) == 0 ) {
         i_data.length = rnd(i_data.length - 8);
         i_data = i_data || Varchar<10>("ORIGINAL");
      }
      item.insert({i, randomId(1, 10000), randomastring<24>(14, 24), randomNumeric(1.00, 100.00), i_data});

   }
}

void loadWarehouse()
{
   for ( Integer i = 0; i < warehouseCount; i++ ) {
      warehouse.insert({i + 1, randomastring<10>(6, 10), randomastring<20>(10, 20), randomastring<20>(10, 20), randomastring<20>(10, 20), randomastring<2>(2, 2), randomzip(), randomNumeric(0.1000, 0.2000), 3000000});
   }
}

void loadStock(Integer w_id)
{
   for ( Integer i = 0; i < 100000; i++ ) {
      Varchar<50> s_data = randomastring<50>(25, 50);
      if ( rnd(10) == 0 ) {
         s_data.length = rnd(s_data.length - 8);
         s_data = s_data || Varchar<10>("ORIGINAL");
      }
      stock.insert({w_id, i + 1, randomNumeric(10, 100), randomastring<24>(24, 24), randomastring<24>(24, 24), randomastring<24>(24, 24), randomastring<24>(24, 24), randomastring<24>(24, 24), randomastring<24>(24, 24), randomastring<24>(24, 24), randomastring<24>(24, 24), randomastring<24>(24, 24), randomastring<24>(24, 24), 0, 0, 0, s_data});
   }
}

void loadDistrinct(Integer w_id)
{
   for ( Integer i = 1; i < 11; i++ ) {
      district.insert({w_id, i, randomastring<10>(6, 10), randomastring<20>(10, 20), randomastring<20>(10, 20), randomastring<20>(10, 20), randomastring<2>(2, 2), randomzip(), randomNumeric(0.0000, 0.2000), 3000000, 3001});
   }
}

Integer h_pk = 1;

Timestamp currentTimestamp()
{
   return 1;
}

void loadCustomer(Integer w_id, Integer d_id)
{
   Timestamp now = currentTimestamp();
   for ( Integer i = 0; i < 3000; i++ ) {
      Varchar<16> c_last;
      if ( i < 1000 )
         c_last = genName(i);
      else
         c_last = genName(nurand(255, 0, 999));
      Varchar<16> c_first = randomastring<16>(8, 16);
      Varchar<2> c_credit(rnd(10) ? "GC" : "BC");
      customer.insert({w_id, d_id, i + 1, c_first, "OE", c_last, randomastring<20>(10, 20), randomastring<20>(10, 20), randomastring<20>(10, 20), randomastring<2>(2, 2), randomzip(), randomnstring(16, 16), now, c_credit, 50000.00, randomNumeric(0.0000, 0.5000), -10.00, 1, 0, 0, randomastring<500>(300, 500)});
      customerwdl.insert({w_id, d_id, c_last, c_first, i + 1});
      history.insert({h_pk++, i + 1, d_id, w_id, d_id, w_id, now, 10.00, randomastring<24>(12, 24)});
   }
}

void loadOrders(Integer w_id, Integer d_id)
{
   Timestamp now = currentTimestamp();
   vector<Integer> c_ids;
   for ( Integer i = 1; i <= 3000; i++ )
      c_ids.push_back(i);
   random_shuffle(c_ids.begin(), c_ids.end());
   Integer o_id = 1;
   for ( Integer o_c_id : c_ids ) {
      Integer o_carrier_id = (o_id < 2101) ? rnd(10) + 1 : 0;
      Numeric o_ol_cnt = rnd(10) + 5;

      order.insert({w_id, d_id, o_id, o_c_id, now, o_carrier_id, o_ol_cnt, 1});
      order_wdc.insert({w_id, d_id, o_c_id, o_id});

      for ( Integer ol_number = 1; ol_number <= o_ol_cnt; ol_number++ ) {
         Timestamp ol_delivery_d = 0;
         if ( o_id < 2101 )
            ol_delivery_d = now;
         Numeric ol_amount = (o_id < 2101) ? 0 : randomNumeric(0.01, 9999.99);
         orderline.insert({w_id, d_id, o_id, ol_number, rnd(100000) + 1, w_id, ol_delivery_d, 5, ol_amount, randomastring<24>(24, 24)});
      }
      o_id++;
   }

   for ( Integer i = 2100; i <= 3000; i++ )
      neworder.insert({w_id, d_id, i});
}

void load()
{
   loadItem();
   loadWarehouse();
   for ( Integer w_id = 1; w_id <= warehouseCount; w_id++ ) {
      loadStock(w_id);
      loadDistrinct(w_id);
      for ( Integer d_id = 1; d_id <= 10; d_id++ ) {
         loadCustomer(w_id, d_id);
         loadOrders(w_id, d_id);
      }
   }
}

// run

Integer urand(Integer low, Integer high) { return rnd(high - low + 1) + low; }

Integer urandexcept(Integer low, Integer high, Integer v)
{
   if ( high <= low )
      return low;
   Integer r = rnd(high - low) + low;
   if ( r >= v )
      return r + 1;
   else
      return r;
}

void newOrder(Integer w_id, Integer d_id, Integer c_id,
              const vector<Integer> &lineNumbers, const vector<Integer> &supwares, const vector<Integer> &itemids, const vector<Integer> &qtys,
              Timestamp timestamp)
{
   Numeric w_tax = warehouse.lookupField({w_id}, &warehouse_t::w_tax);
   Numeric c_discount = customer.lookupField({w_id, d_id, c_id}, &customer_t::c_discount);
   Numeric d_tax;
   Integer o_id;
   district.lookup1({w_id, d_id}, [&](const district_t &rec) {
      o_id = rec.d_next_o_id;
      d_tax = rec.d_tax;
   });
   district.update1({w_id, d_id}, [&](district_t &rec) { rec.d_next_o_id++; });

   Numeric all_local = 1;
   for ( Integer sw : supwares )
      if ( sw != w_id )
         all_local = 0;
   Numeric cnt = lineNumbers.size();
   Integer carrier_id = 0; /*null*/
   order.insert({w_id, d_id, o_id, c_id, timestamp, carrier_id, cnt, all_local});
   order_wdc.insert({w_id, d_id, c_id, o_id});
   neworder.insert({w_id, d_id, o_id});

   for ( unsigned i = 0; i < lineNumbers.size(); i++ ) {
      Integer qty = qtys[i];
      stock.update1({supwares[i], itemids[i]}, [&](stock_t &rec) {
         auto &s_quantity = rec.s_quantity;
         s_quantity = (s_quantity >= qty + 10) ? s_quantity - qty : s_quantity + 91 - qty;
         rec.s_remote_cnt += (supwares[i] != w_id);
         rec.s_order_cnt++;
         rec.s_ytd += qty;
      });
   }

   for ( unsigned i = 0; i < lineNumbers.size(); i++ ) {
      Integer lineNumber = lineNumbers[i];
      Integer supware = supwares[i];
      Integer itemid = itemids[i];
      Numeric qty = qtys[i];

      Numeric i_price = item.lookupField({itemid}, &item_t::i_price); // TODO: rollback on miss
      Varchar<24> s_dist;
      stock.lookup1({w_id, itemid}, [&](const stock_t &rec) {
         switch ( d_id ) {
            case 1:
               s_dist = rec.s_dist_01;
               break;
            case 2:
               s_dist = rec.s_dist_02;
               break;
            case 3:
               s_dist = rec.s_dist_03;
               break;
            case 4:
               s_dist = rec.s_dist_04;
               break;
            case 5:
               s_dist = rec.s_dist_05;
               break;
            case 6:
               s_dist = rec.s_dist_06;
               break;
            case 7:
               s_dist = rec.s_dist_07;
               break;
            case 8:
               s_dist = rec.s_dist_08;
               break;
            case 9:
               s_dist = rec.s_dist_09;
               break;
            case 10:
               s_dist = rec.s_dist_10;
               break;
            default:
               throw;
         }
      });
      Numeric ol_amount = qty * i_price * (1.0 + w_tax + d_tax) * (1.0 - c_discount);
      Timestamp ol_delivery_d = 0; // NULL
      orderline.insert({w_id, d_id, o_id, lineNumber, itemid, supware, ol_delivery_d, qty, ol_amount, s_dist});
      // TODO: i_data, s_data
   }
}

void newOrderRnd(Integer w_id)
{
   Integer d_id = urand(1, 10);
   Integer c_id = nurand(1023, 1, 3000);
   Integer ol_cnt = urand(5, 15);

   vector<Integer> lineNumbers;
   lineNumbers.reserve(15);
   vector<Integer> supwares;
   supwares.reserve(15);
   vector<Integer> itemids;
   itemids.reserve(15);
   vector<Integer> qtys;
   qtys.reserve(15);
   for ( Integer i = 1; i <= ol_cnt; i++ ) {
      Integer supware = w_id;
      if ( urand(1, 100) == 1 ) // remote transaction
         supware = urandexcept(1, warehouseCount, w_id);
      Integer itemid = nurand(8191, 1, 100000);
      if ( false && (i == ol_cnt) && (urand(1, 100) == 1)) // invalid item => random
         itemid = 0;
      lineNumbers.push_back(i);
      supwares.push_back(supware);
      itemids.push_back(itemid);
      qtys.push_back(urand(1, 10));
   }
   newOrder(w_id, d_id, c_id, lineNumbers, supwares, itemids, qtys, currentTimestamp());
}

void delivery(Integer w_id, Integer carrier_id, Timestamp datetime)
{
   for ( Integer d_id = 1; d_id < 10; d_id++ ) {
      Integer o_id = minInteger;
      neworder.scan({w_id, d_id, minInteger}, [&](const neworder_t &rec) {
         if ( rec.no_w_id == w_id && rec.no_d_id == d_id )
            o_id = rec.no_o_id;
         return false;
      }, [] (){});
      if ( o_id == minInteger )
         continue;
      neworder.erase({w_id, d_id, o_id});

      //Integer ol_cnt = minInteger, c_id;
      //order.scan({w_id, d_id, o_id}, [&](const order_t& rec) { ol_cnt = rec.o_ol_cnt; c_id = rec.o_c_id; return false; });
      //if (ol_cnt == minInteger)
      // continue;
      Integer ol_cnt, c_id;
      order.lookup1({w_id, d_id, o_id}, [&](const order_t &rec) {
         ol_cnt = rec.o_ol_cnt;
         c_id = rec.o_c_id;
         return false;
      });
      order.update1({w_id, d_id, o_id}, [&](order_t &rec) { rec.o_carrier_id = carrier_id; });

      Numeric ol_total = 0;
      for ( Integer ol_number = 1; ol_number <= ol_cnt; ol_number++ ) {
         ol_total += orderline.lookupField({w_id, d_id, o_id, ol_number}, &orderline_t::ol_amount);
         orderline.update1({w_id, d_id, o_id, ol_number}, [&](orderline_t &rec) { rec.ol_delivery_d = datetime; });
      }

      customer.update1({w_id, d_id, c_id}, [&](customer_t &rec) {
         rec.c_balance += ol_total;
         rec.c_delivery_cnt++;
      });
   }
}

void deliveryRnd(Integer w_id)
{
   Integer carrier_id = urand(1, 10);
   delivery(w_id, carrier_id, currentTimestamp());
}

void stockLevel(Integer w_id, Integer d_id, Integer threshold)
{
   Integer o_id = district.lookupField({w_id, d_id}, &district_t::d_next_o_id);

   //"SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT FROM orderline, stock WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID < ? AND OL_O_ID >= ? AND S_W_ID = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?"
   vector<Integer> items;
   items.reserve(100);
   orderline.scan({w_id, d_id, o_id - 20, minInteger}, [&](const orderline_t &rec) {
      if ( rec.ol_w_id == w_id && rec.ol_d_id == d_id && rec.ol_o_id < o_id ) {
         items.push_back(rec.ol_i_id);
         return true;
      }
      return false;
   }, [] (){});
   std::sort(items.begin(), items.end());
   std::unique(items.begin(), items.end());
   unsigned count = 0;
   for ( Integer i_id : items )
      stock.lookup1({w_id, i_id}, [&](const stock_t &rec) { count += (rec.s_quantity < threshold); });
}

void stockLevelRnd(Integer w_id)
{
   stockLevel(w_id, urand(1, 10), urand(10, 20));
}

void orderStatusId(Integer w_id, Integer d_id, Integer c_id)
{
   Varchar<16> c_first;
   Varchar<2> c_middle;
   Varchar<16> c_last;
   Numeric c_balance;
   customer.lookup1({w_id, d_id, c_id}, [&](const customer_t &rec) {
      c_first = rec.c_first;
      c_middle = rec.c_middle;
      c_last = rec.c_last;
      c_balance = rec.c_balance;
   });
   Integer o_id = minInteger;
   order_wdc.scan({w_id, d_id, c_id, minInteger}, [&](const order_wdc_t &rec) {
      if ((rec.o_w_id == w_id) && (rec.o_d_id == d_id) && (rec.o_c_id == c_id)) {
         o_id = rec.o_id;
         return true;
      }
      return false;
   }, [] (){});
   if ( o_id == minInteger )
      return; // is this correct?

   Timestamp o_entry_d;
   Integer o_carrier_id;

   order.lookup1({w_id, d_id, o_id}, [&](const order_t &rec) {
      o_entry_d = rec.o_entry_d;
      o_carrier_id = rec.o_carrier_id;
   });
   Integer ol_i_id;
   Integer ol_supply_w_id;
   Timestamp ol_delivery_d;
   Numeric ol_quantity;
   Numeric ol_amount;
   orderline.scan({w_id, d_id, o_id, minInteger}, [&](const orderline_t &rec) {
      if ( rec.ol_w_id == w_id && rec.ol_d_id == d_id && rec.ol_o_id == o_id ) {
         ol_i_id = rec.ol_i_id;
         ol_supply_w_id = rec.ol_supply_w_id;
         ol_delivery_d = rec.ol_delivery_d;
         ol_quantity = rec.ol_quantity;
         ol_amount = rec.ol_amount;
         return true;
      }
      return false;
   }, [] (){});

}

void orderStatusName(Integer w_id, Integer d_id, Varchar<16> c_last)
{
   vector<Integer> ids;
   customerwdl.scan({w_id, d_id, c_last, {}}, [&](const customer_wdl_t &rec) {
      if ( rec.c_w_id == w_id && rec.c_d_id == d_id && rec.c_last == c_last ) {
         ids.push_back(rec.c_id);
         return true;
      }
      return false;
   }, [] (){});
   unsigned c_count = ids.size();
   if ( c_count == 0 )
      return; // TODO: rollback
   unsigned index = c_count / 2;
   if ((c_count % 2) == 0 )
      index -= 1;
   Integer c_id = ids[index];

   Integer o_id = minInteger;
   order_wdc.scan({w_id, d_id, c_id, minInteger}, [&](const order_wdc_t &rec) {
      if ( rec.o_w_id == w_id && rec.o_d_id == d_id && rec.o_c_id == c_id ) {
         o_id = rec.o_id;
         return true;
      }
      return false;
   }, [] (){});
   if ( o_id == minInteger ) // TODO: is this correct?
      return;
   Timestamp ol_delivery_d;
   orderline.scan({w_id, d_id, o_id, minInteger}, [&](const orderline_t &rec) {
      if ( rec.ol_w_id == w_id && rec.ol_d_id == d_id && rec.ol_o_id == o_id ) {
         ol_delivery_d = rec.ol_delivery_d;
         return true;
      }
      return false;
   }, [] (){});
}

void orderStatusRnd(Integer w_id)
{
   Integer d_id = urand(1, 10);
   if ( urand(1, 100) <= 40 ) {
      orderStatusId(w_id, d_id, nurand(1023, 1, 3000));
   } else {
      orderStatusName(w_id, d_id, genName(nurand(255, 0, 999)));
   }
}

void paymentById(Integer w_id, Integer d_id, Integer c_w_id, Integer c_d_id, Integer c_id, Timestamp h_date, Numeric h_amount, Timestamp datetime)
{
   Varchar<10> w_name;
   Varchar<20> w_street_1;
   Varchar<20> w_street_2;
   Varchar<20> w_city;
   Varchar<2> w_state;
   Varchar<9> w_zip;
   Numeric w_ytd;
   warehouse.lookup1({w_id}, [&](const warehouse_t &rec) {
      w_name = rec.w_name;
      w_street_1 = rec.w_street_1;
      w_street_2 = rec.w_street_2;
      w_city = rec.w_city;
      w_state = rec.w_state;
      w_zip = rec.w_zip;
      w_ytd = rec.w_ytd;
   });
   warehouse.update1({w_id}, [&](warehouse_t &rec) { rec.w_ytd += h_amount; });

   Varchar<10> d_name;
   Varchar<20> d_street_1;
   Varchar<20> d_street_2;
   Varchar<20> d_city;
   Varchar<2> d_state;
   Varchar<9> d_zip;
   Numeric d_ytd;
   district.lookup1({w_id, d_id}, [&](const district_t &rec) {
      d_name = rec.d_name;
      d_street_1 = rec.d_street_1;
      d_street_2 = rec.d_street_2;
      d_city = rec.d_city;
      d_state = rec.d_state;
      d_zip = rec.d_zip;
      d_ytd = rec.d_ytd;
   });
   district.update1({w_id, d_id}, [&](district_t &rec) { rec.d_ytd += h_amount; });

   Varchar<500> c_data;
   Varchar<2> c_credit;
   Numeric c_balance;
   Numeric c_ytd_payment;
   Numeric c_payment_cnt;
   customer.lookup1({c_w_id, c_d_id, c_id}, [&](const customer_t &rec) {
      c_data = rec.c_data;
      c_credit = rec.c_credit;
      c_balance = rec.c_balance;
      c_ytd_payment = rec.c_ytd_payment;
      c_payment_cnt = rec.c_payment_cnt;
   });
   Numeric c_new_balance = c_balance - h_amount;
   Numeric c_new_ytd_payment = c_ytd_payment + h_amount;
   Numeric c_new_payment_cnt = c_payment_cnt + 1;

   if ( c_credit == "BC" ) {
      Varchar<500> c_new_data;
      auto numChars = snprintf(c_new_data.data, 500, "| %4d %2d %4d %2d %4d $%7.2f %lu %s%s %s", c_id, c_d_id, c_w_id, d_id, w_id, h_amount, h_date, w_name.toString().c_str(), d_name.toString().c_str(), c_data.toString().c_str());
      c_new_data.length = numChars;
      if ( c_new_data.length > 500 )
         c_new_data.length = 500;
      customer.update1({c_w_id, c_d_id, c_id}, [&](customer_t &rec) {
         rec.c_data = c_new_data;
         rec.c_balance = c_new_balance;
         rec.c_ytd_payment = c_new_ytd_payment;
         rec.c_payment_cnt = c_new_payment_cnt;
      });
   } else {
      customer.update1({c_w_id, c_d_id, c_id}, [&](customer_t &rec) {
         rec.c_balance = c_new_balance;
         rec.c_ytd_payment = c_new_ytd_payment;
         rec.c_payment_cnt = c_new_payment_cnt;
      });
   }

   Varchar<24> h_new_data = Varchar<24>(w_name) || Varchar<24>("    ") || d_name;
   history.insert({h_pk++, c_id, c_d_id, c_w_id, d_id, w_id, datetime, h_amount, h_new_data});
}

void paymentByName(Integer w_id, Integer d_id, Integer c_w_id, Integer c_d_id, Varchar<16> c_last, Timestamp h_date, Numeric h_amount, Timestamp datetime)
{
   Varchar<10> w_name;
   Varchar<20> w_street_1;
   Varchar<20> w_street_2;
   Varchar<20> w_city;
   Varchar<2> w_state;
   Varchar<9> w_zip;
   Numeric w_ytd;
   warehouse.lookup1({w_id}, [&](const warehouse_t &rec) {
      w_name = rec.w_name;
      w_street_1 = rec.w_street_1;
      w_street_2 = rec.w_street_2;
      w_city = rec.w_city;
      w_state = rec.w_state;
      w_zip = rec.w_zip;
      w_ytd = rec.w_ytd;
   });
   warehouse.update1({w_id}, [&](warehouse_t &rec) { rec.w_ytd += h_amount; });
   Varchar<10> d_name;
   Varchar<20> d_street_1;
   Varchar<20> d_street_2;
   Varchar<20> d_city;
   Varchar<2> d_state;
   Varchar<9> d_zip;
   Numeric d_ytd;
   district.lookup1({w_id, d_id}, [&](const district_t &rec) {
      d_name = rec.d_name;
      d_street_1 = rec.d_street_1;
      d_street_2 = rec.d_street_2;
      d_city = rec.d_city;
      d_state = rec.d_state;
      d_zip = rec.d_zip;
      d_ytd = rec.d_ytd;
   });
   district.update1({w_id, d_id}, [&](district_t &rec) { rec.d_ytd += h_amount; });

   // Get customer id by name
   vector<Integer> ids;
   customerwdl.scan({c_w_id, c_d_id, c_last, {}}, [&](const customer_wdl_t &rec) {
      if ( rec.c_w_id == c_w_id && rec.c_d_id == c_d_id && rec.c_last == c_last ) {
         ids.push_back(rec.c_id);
         return true;
      }
      return false;
   }, [] (){});
   unsigned c_count = ids.size();
   if ( c_count == 0 )
      return; // TODO: rollback
   unsigned index = c_count / 2;
   if ((c_count % 2) == 0 )
      index -= 1;
   Integer c_id = ids[index];

   Varchar<500> c_data;
   Varchar<2> c_credit;
   Numeric c_balance;
   Numeric c_ytd_payment;
   Numeric c_payment_cnt;
   customer.lookup1({c_w_id, c_d_id, c_id}, [&](const customer_t &rec) {
      c_data = rec.c_data;
      c_credit = rec.c_credit;
      c_balance = rec.c_balance;
      c_ytd_payment = rec.c_ytd_payment;
      c_payment_cnt = rec.c_payment_cnt;
   });
   Numeric c_new_balance = c_balance - h_amount;
   Numeric c_new_ytd_payment = c_ytd_payment + h_amount;
   Numeric c_new_payment_cnt = c_payment_cnt + 1;

   if ( c_credit == "BC" ) {
      Varchar<500> c_new_data;
      auto numChars = snprintf(c_new_data.data, 500, "| %4d %2d %4d %2d %4d $%7.2f %lu %s%s %s", c_id, c_d_id, c_w_id, d_id, w_id, h_amount, h_date, w_name.toString().c_str(), d_name.toString().c_str(), c_data.toString().c_str());
      c_new_data.length = numChars;
      if ( c_new_data.length > 500 )
         c_new_data.length = 500;
      customer.update1({c_w_id, c_d_id, c_id}, [&](customer_t &rec) {
         rec.c_data = c_new_data;
         rec.c_balance = c_new_balance;
         rec.c_ytd_payment = c_new_ytd_payment;
         rec.c_payment_cnt = c_new_payment_cnt;
      });
   } else {
      customer.update1({c_w_id, c_d_id, c_id}, [&](customer_t &rec) {
         rec.c_balance = c_new_balance;
         rec.c_ytd_payment = c_new_ytd_payment;
         rec.c_payment_cnt = c_new_payment_cnt;
      });
   }

   Varchar<24> h_new_data = Varchar<24>(w_name) || Varchar<24>("    ") || d_name;
   history.insert({h_pk++, c_id, c_d_id, c_w_id, d_id, w_id, datetime, h_amount, h_new_data});
}

void paymentRnd(Integer w_id)
{
   Integer d_id = urand(1, 10);
   Integer c_w_id = w_id;
   Integer c_d_id = d_id;
   if ( urand(1, 100) > 85 ) {
      c_w_id = urandexcept(1, warehouseCount, w_id);
      c_d_id = urand(1, 10);
   }
   Numeric h_amount = randomNumeric(1.00, 5000.00);
   Timestamp h_date = currentTimestamp();

   if ( urand(1, 100) <= 60 ) {
      paymentByName(w_id, d_id, c_w_id, c_d_id, genName(nurand(255, 0, 999)), h_date, h_amount, currentTimestamp());
   } else {
      paymentById(w_id, d_id, c_w_id, c_d_id, nurand(1023, 1, 3000), h_date, h_amount, currentTimestamp());
   }

}

int tx()
{
   Integer w_id = urand(1, warehouseCount);
   int rnd = random() % 1000;
   if ( rnd < 430 ) {
      paymentRnd(w_id);
      return 0;
   }
   rnd -= 430;
   if ( rnd < 40 ) {
      orderStatusRnd(w_id);
      return 1;
   }
   rnd -= 40;
   if ( rnd < 40 ) {
      deliveryRnd(w_id);
      return 2;
   }
   rnd -= 40;
   if ( rnd < 40 ) {
      stockLevelRnd(w_id);
      return 3;
   }
   rnd -= 40;
   newOrderRnd(w_id);
   return 4;
}

int main(int argc, char **argv)
{
   gflags::SetUsageMessage("Leanstore TPC-C");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   LeanStore db;
   warehouse = LeanStoreAdapter<warehouse_t>(db, "warehouse");
   district = LeanStoreAdapter<district_t>(db, "district");
   customer = LeanStoreAdapter<customer_t>(db, "customer");
   customerwdl = LeanStoreAdapter<customer_wdl_t>(db, "customerwdl");
   history = LeanStoreAdapter<history_t>(db, "history");
   neworder = LeanStoreAdapter<neworder_t>(db, "neworder");
   order = LeanStoreAdapter<order_t>(db, "order");
   order_wdc = LeanStoreAdapter<order_wdc_t>(db, "order_wdc");
   orderline = LeanStoreAdapter<orderline_t>(db, "orderline");
   item = LeanStoreAdapter<item_t>(db, "item");
   stock = LeanStoreAdapter<stock_t>(db, "stock");
   // -------------------------------------------------------------------------------------
   load();

   cout << "warehouse: " << warehouse.count() << endl;
   cout << "district: " << district.count() << endl;
   cout << "customer: " << customer.count() << endl;
   cout << "customerwdl: " << customerwdl.count() << endl;
   cout << "history: " << history.count() << endl;
   cout << "neworder: " << neworder.count() << endl;
   cout << "order: " << order.count() << endl;
   cout << "order_wdc: " << order_wdc.count() << endl;
   cout << "orderline: " << orderline.count() << endl;
   cout << "item: " << item.count() << endl;
   cout << "stock: " << stock.count() << endl;
   cout << endl;

   PerfEvent e;
   for ( unsigned j = 0; j < 10; j++ ) {
      unsigned n = 10000;
      {
         PerfEventBlock b(e, n);
         for ( unsigned i = 0; i < n; i++ ) {
            tx();
         }
      }
   }

   cout << "warehouse: " << warehouse.count() << endl;
   cout << "district: " << district.count() << endl;
   cout << "customer: " << customer.count() << endl;
   cout << "customerwdl: " << customerwdl.count() << endl;
   cout << "history: " << history.count() << endl;
   cout << "neworder: " << neworder.count() << endl;
   cout << "order: " << order.count() << endl;
   cout << "order_wdc: " << order_wdc.count() << endl;
   cout << "orderline: " << orderline.count() << endl;
   cout << "item: " << item.count() << endl;
   cout << "stock: " << stock.count() << endl;
   cout << endl;

   return 0;
}
