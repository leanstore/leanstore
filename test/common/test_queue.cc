#include "common/queue.h"
#include "transaction/transaction.h"

#include "gtest/gtest.h"

#include <thread>

enum { NO_OPERATIONS = 1000000, OBJECT_SIZE = sizeof(leanstore::transaction::SerializableTransaction) };

namespace leanstore {

TEST(TestQueue, BasicTest) {
  LockFreeQueue<transaction::SerializableTransaction> queue;

  queue.Push(transaction::Transaction());
  EXPECT_EQ(queue.tail_.load(), OBJECT_SIZE);

  queue.Push(transaction::Transaction());
  EXPECT_EQ(queue.head_.load(), 0);
  EXPECT_EQ(queue.tail_.load(), 2 * OBJECT_SIZE);

  auto n_items  = 0;
  auto no_bytes = queue.LoopElements(2 * OBJECT_SIZE, [&]([[maybe_unused]] auto &txn) {
    n_items++;
    return true;
  });
  EXPECT_EQ(n_items, 2);
  EXPECT_EQ(no_bytes, queue.tail_.load());
  queue.Erase(no_bytes);
  EXPECT_EQ(queue.head_.load(), queue.tail_.load());

  queue.Push(transaction::Transaction());
  no_bytes = queue.LoopElements(3 * OBJECT_SIZE, [&]([[maybe_unused]] auto &txn) { return true; });
  EXPECT_EQ(no_bytes, OBJECT_SIZE);
  EXPECT_EQ(queue.tail_.load(), 3 * OBJECT_SIZE);
}

TEST(TestQueue, ConcurrencyTest) {
  LockFreeQueue<transaction::SerializableTransaction> queue;

  auto producer = std::thread([&]() {
    for (auto idx = 0; idx < NO_OPERATIONS; idx++) { queue.Push(transaction::Transaction()); }
  });

  auto consumer = std::thread([&]() {
    auto total_items = 0UL;
    for (; total_items < NO_OPERATIONS;) {
      auto cur_head = queue.head_.load();
      auto cur_tail = queue.tail_.load();
      while (cur_head == cur_tail) { cur_tail = queue.tail_.load(); }
      auto loop_items = 0UL;
      auto loop_bytes = queue.LoopElements(cur_tail, [&](auto &txn) {
        if (loop_items > 10) { return false; }  // Only visit at most 10 objects to shorten the loop
        EXPECT_EQ(txn.state, transaction::Transaction::State::IDLE);
        EXPECT_EQ(txn.commit_ts, 0);
        loop_items++;
        return true;
      });
      total_items += loop_items;
      EXPECT_GE(loop_items, 1);
      /**
       * Most of the time, the previous condition is true
       * Sometimes, the cursor circulars back to the beginning, hence
       *  the padding == OBJECT_SIZE, i.e., the latter equation is true instead
       */
      EXPECT_TRUE((loop_bytes == loop_items * OBJECT_SIZE) || (loop_bytes == (loop_items + 1) * OBJECT_SIZE));
      queue.Erase(loop_bytes);
    }
  });

  producer.join();
  consumer.join();
  EXPECT_EQ(queue.head_.load(), queue.tail_.load());
}

}  // namespace leanstore

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}