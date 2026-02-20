#pragma once

/** Stolen and modified from https://github.com/rotaki/tpcc-runner/blob/master/protocols/waitdie/include/waitdielock.hpp
 */

#include "common/typedefs.h"
#include "common/utils.h"

#include "fmt/ranges.h"
#include "spdlog/spdlog.h"

#include <cassert>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>

namespace leanstore::transaction::svcc {

struct WaiterNode;
struct OwnerNode;

template <typename Node>
struct TimestampSortedList {
 private:
  struct Item {
    template <typename... Args>
    Item(timestamp_t ts, Args &&...args) : ts(ts), node(std::forward<Args>(args)...) {}

    timestamp_t ts;
    Node node;
  };

 public:
  // Constructors / Queries
  TimestampSortedList() = default;

  bool Empty() const { return item_list.empty(); }

  void Clear() { item_list.clear(); }

  size_t Size() const { return item_list.size(); }

  // Insert in descending timestamp order
  template <typename... Args>
  auto Insert(timestamp_t ts, Args &&...args) -> Node * {
    for (auto iter = item_list.begin(); iter != item_list.end(); ++iter) {
      if (iter->ts < ts) {  // insert before first smaller timestamp
        auto it = item_list.emplace(iter, ts, std::forward<Args>(args)...);
        return &((*it).node);  // pointer to newly inserted Item
      }
    }
    // insert at back
    assert(item_list.empty() || item_list.back().ts >= ts);
    auto &ref = item_list.emplace_back(ts, std::forward<Args>(args)...);
    return &(ref.node);  // pointer to newly inserted Item
  }

  // Remove first item with matching timestamp
  void Remove(timestamp_t ts) {
    for (auto iter = item_list.begin(); iter != item_list.end(); ++iter) {
      if (iter->ts == ts) {
        item_list.erase(iter);
        return;
      }
    }
    throw std::runtime_error("timestamp not in list");
  }

  // Accessors
  Node &Front() { return item_list.front().node; }

  void Pop() { item_list.pop_front(); }

  timestamp_t GetBackTimestamp() const { return item_list.back().ts; }

  // Debug / Logging
  std::string ToString() const {
    std::vector<std::string> items;
    items.reserve(item_list.size());

    for (const auto &item : item_list) {
      if constexpr (std::is_same_v<Node, WaiterNode>) {
        items.push_back(fmt::format("W(ts={}, op={}, waiting={})", item.ts, static_cast<int>(item.node.op),
                                    item.node.waiting.load(std::memory_order_relaxed)));
      } else {
        // OwnerNode
        items.push_back(fmt::format("O(ts={})", item.ts));
      }
    }

    return fmt::format("[{}]", fmt::join(items, ", "));
  }

 private:
  std::list<Item> item_list;
};

class WaitDieLock {
 private:
  enum Operation : uint8_t {
    I,  // invalid
    S,  // shared
    E,  // exclusive
    U   // upgrade
  };

  struct WaiterNode {
    WaiterNode(timestamp_t ts, Operation op, bool waiting) : waiting(waiting), ts(ts), op(op) {}

    alignas(64) std::atomic<bool> waiting;  // spin variable
    char pad[64 - sizeof(bool)] = {};
    timestamp_t ts;  // timestamp
    Operation op;    // S, E, U
  };

  // sorted (ts big -> ts small) list of waiters
  using WaiterList = TimestampSortedList<WaiterNode>;

  struct OwnerNode {
    OwnerNode(timestamp_t ts) : ts(ts) {}

    timestamp_t ts;
  };

  struct OwnerList {
    Operation op = I;                       // I, S, E
    TimestampSortedList<OwnerNode> owners;  // sorted (ts big -> ts small) list of owners

    void Insert(timestamp_t ts) { owners.Insert(ts, ts); }

    void Remove(timestamp_t ts) {
      owners.Remove(ts);
      if (owners.Empty()) { op = I; }
    }

    auto Size() { return owners.Size(); }

    // get the smallest timestamp in owner_list
    auto MinTimestamp() { return owners.GetBackTimestamp(); }

    auto ToString() {
      const char *prefix = (op == I ? "(I):" : op == S ? "(S):" : "(E):");
      return fmt::format("{}{}", prefix, owners.ToString());
    };
  };

  std::mutex latch;
  WaiterList waiter_list;
  OwnerList owner_list;

 public:
  WaitDieLock() {}

  void Trace() {
    std::lock_guard<std::mutex> guard(latch);
    TraceWithoutLatch();
  }

  void TraceWithoutLatch() { spdlog::debug("Waiter: {}; Owner: {}", waiter_list.ToString(), owner_list.ToString()); }

  bool TryLockShared(uint64_t ts) {
    std::unique_lock<std::mutex> guard(latch);
    /**
     * STATE -> ACTION
     *
     * owner(I, S), no waiter -> add to owner_list, change it's op to S, unlock latch and return
     *true
     *
     * owner(I), waiter -> add to waiter_list, unlock latch and spin
     *
     * owner(S), waiter -> compare with min ts of owner with this ts, if this ts is smaller, add
     *to waiter_list, unlock latch and spin else unlock latch and return false
     *
     * owner(E) -> compare with min ts of owner with this ts, if this ts is smaller, add to
     *waiter_list, unlock latch and spin else unlock latch and return false
     **/
    bool no_waiter = waiter_list.Empty();
    Operation op   = owner_list.op;
    if ((op == I || op == S) && no_waiter) {
      owner_list.Insert(ts);
      owner_list.op = S;
      return true;
    }
    if ((op == I) && !no_waiter) {
      auto node = waiter_list.Insert(ts, ts, S, true);
      guard.unlock();
      while (node->waiting.load(std::memory_order_acquire)) { AsmYield(); }  // Spinning
      return true;
    }
    if (((op == S) && !no_waiter) || op == E) {
      if (owner_list.MinTimestamp() > ts) {
        auto node = waiter_list.Insert(ts, ts, S, true);
        guard.unlock();
        while (node->waiting.load(std::memory_order_acquire)) { AsmYield(); }  // Spinning
        return true;
      } else {
        return false;
      }
    }
    throw std::runtime_error("Unhandled State Found");
  };

  bool TryLock(uint64_t ts) {
    std::unique_lock<std::mutex> guard(latch);
    /**
     * STATE -> ACTION
     *
     * owner(I), no waiter -> add to owner_list, change it's op to E, unlock latch and return
     *true
     *
     * owner(I), waiter -> add to waiter_list, unlock latch and spin
     *
     * owner(S, E) -> compare with min ts of owner with this ts, if this ts is smaller, add to
     *waiter_list, unlock latch and spin else unlock latch and return false
     *
     **/
    bool no_waiter = waiter_list.Empty();
    Operation op   = owner_list.op;
    if (op == I && no_waiter) {
      // add to owner_list and return
      owner_list.Insert(ts);
      owner_list.op = E;
      return true;
    }
    if (op == I && !no_waiter) {
      // add to waiter_list and spin
      auto node = waiter_list.Insert(ts, ts, E, true);
      guard.unlock();
      while (node->waiting.load(std::memory_order_acquire)) { AsmYield(); }
      return true;
    }
    if (op == S || op == E) {
      if (owner_list.MinTimestamp() > ts) {
        auto node = waiter_list.Insert(ts, ts, E, true);
        guard.unlock();
        while (node->waiting.load(std::memory_order_acquire)) { AsmYield(); }
        return true;
      } else {
        return false;
      }
    }
    throw std::runtime_error("Unhandled State Found");
  }

  bool TryLockUpgrade(uint64_t ts) {
    std::unique_lock<std::mutex> guard(latch);
    /**
     * STATE -> ACTION
     *
     * owner(I, E) -> throw
     *
     * owner(S) -> compare with min ts of owner with this ts, if they are same, and multiple
     *owners exist, add to waiter_list, unlock latch and spin if they are same, and single owner
     *exists, change owner state and return true else unlock latch and return false
     **/
    Operation op = owner_list.op;
    if (op == I || op == E) {
      throw std::runtime_error("No lock to upgrade");
    } else if (op == S) {
      auto min_ts         = owner_list.MinTimestamp();
      uint64_t num_owners = owner_list.Size();
      if (min_ts == ts && num_owners > 1) {
        auto node = waiter_list.Insert(ts, ts, U, true);  // this should come to the head of waiter_list
        guard.unlock();
        while (node->waiting.load(std::memory_order_acquire)) { AsmYield(); }
        return true;
      } else if (min_ts == ts && num_owners == 1) {
        owner_list.op = E;
        return true;
      } else {
        return false;
      }
    }
    throw std::runtime_error("Unhandled State Found");
  }

  void UnlockShared(uint64_t ts) {
    std::unique_lock<std::mutex> guard(latch);
    /**
     * STATE -> ACTION
     *
     * owner(I, E) -> throw
     *
     * owner(S) -> remove this ts from owner_list, promote waiters, unlock latch, return
     **/
    Operation op = owner_list.op;
    if (op == I || op == E) {
      throw std::runtime_error("No shared lock to unlock");
    } else if (op == S) {
      owner_list.Remove(ts);
      PromoteWaiters();
      return;
    }
    throw std::runtime_error("Unhandled State Found");
  }

  void Unlock(uint64_t ts) {
    std::unique_lock<std::mutex> guard(latch);
    /**
     * STATE -> ACTION
     *
     * owner(I, S) -> throw
     *
     * owner(E) -> remove this ts from owners if found, PromoteWaiters(), unlock latch, return
     **/
    Operation op = owner_list.op;
    if (op == I || op == S) {
      throw std::runtime_error("No exclusive lock to unlock");
    } else if (op == E) {
      owner_list.Remove(ts);
      PromoteWaiters();
      return;
    }
    throw std::runtime_error("Unhandled State Found");
  }

 private:
  // This function requires the caller to already hold exclusive latch
  void PromoteWaiters() {
    /**
     * STATE -> ACTION
     * no_waiter -> return
     *
     * waiter(S), owner(I, S) ->
     *    promote waiter,
     *    change owner mode,
     *    pop from waiter_list,
     *    set waiting to false, spin loop
     *
     * waiter(S), owner(E) -> return waiter(E), owner(S, E) -> return
     *
     * waiter(E), owner(I) ->
     *    promote waiter,
     *    change owner mode,
     *    pop from waiter_list,
     *    set waiting to false, spin loop
     *
     * waiter(U), owner(I, E) -> throw
     *
     * waiter(U), owner(S), multiple owners -> return
     *
     * waiter(U), owner(S), single owner ->
     *    promote waiter,
     *    change owner mode,
     *    pop from waiter_list,
     *    set waiting to false, spin loop
     **/
    Operation o_op;
    Operation w_op;
    uint64_t num_owners;
    while (true) {
      if (waiter_list.Empty()) { return; }

      o_op       = owner_list.op;
      num_owners = owner_list.Size();
      w_op       = waiter_list.Front().op;

      bool finish = (w_op == S && o_op == E) || (w_op == E && (o_op == S || o_op == E)) ||
                    (w_op == U && o_op == S && num_owners > 1);
      if (finish) { return; }

      bool error = (w_op == U && (o_op == I || o_op == E));
      if (error) { throw std::runtime_error("Failed Promoting Waiter"); }

      // loop
      if (w_op == S && (o_op == I || o_op == S)) {
        // promote waiter
        auto &waiter   = waiter_list.Front();
        timestamp_t ts = waiter.ts;
        owner_list.Insert(ts);
        owner_list.op = S;
        // pop
        waiter_list.Pop();
        // set waiting to false
        waiter.waiting.store(false, std::memory_order_release);
        // loop
        continue;
      } else if (w_op == E && o_op == I) {
        // promote waiter
        auto &waiter   = waiter_list.Front();
        timestamp_t ts = waiter.ts;
        owner_list.Insert(ts);
        owner_list.op = E;
        // pop
        waiter_list.Pop();
        // set waiting to false
        waiter.waiting.store(false, std::memory_order_release);
        // loop
        continue;
      } else if (w_op == U && o_op == S && num_owners == 1) {
        // promote waiter
        auto &waiter = waiter_list.Front();
        assert(owner_list.MinTimestamp() == waiter.ts);
        owner_list.op = E;
        // pop
        waiter_list.Pop();
        // set waiting to false
        waiter.waiting.store(false, std::memory_order_release);
        continue;
      }
      throw std::runtime_error("Unhandled State Found");
    }
  }
};

}  // namespace leanstore::transaction::svcc
