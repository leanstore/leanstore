#include "common/queue.h"
#include "transaction/transaction.h"

#include "gtest/gtest.h"

#include <thread>

enum { NO_OPERATIONS = 1000000 };

namespace leanstore {

TEST(TestQueue, BasicTest) {
  LockFreeQueue<transaction::SerializableTransaction> queue;

  queue.Push(transaction::Transaction());
  EXPECT_EQ(queue.tail_.load(), queue.NewTail(sizeof(transaction::SerializableTransaction), 1));

  queue.Push(transaction::Transaction());
  EXPECT_EQ(queue.head_.load(), 0);
  EXPECT_EQ(queue.tail_.load(), queue.NewTail(2 * (sizeof(transaction::SerializableTransaction)), 2));
  EXPECT_EQ(queue.SizeApprox(), 2);

  auto n_items = queue.LoopElement(2, [&]([[maybe_unused]] auto &txn) { return true; });
  EXPECT_EQ(n_items, 2);
  EXPECT_EQ(queue.last_loop_bytes, queue.tail_.load() >> 32);

  queue.Erase(2);
  EXPECT_EQ(queue.last_loop_bytes, 0);
  EXPECT_EQ(queue.head_.load(), queue.tail_.load() >> 32);

  queue.Push(transaction::Transaction());
  n_items = queue.LoopElement(1, [&]([[maybe_unused]] auto &txn) { return true; });
  EXPECT_EQ(n_items, 1);
  EXPECT_EQ(queue.last_loop_bytes, sizeof(transaction::SerializableTransaction));
  EXPECT_EQ(queue.tail_.load(), queue.NewTail(3 * (sizeof(transaction::SerializableTransaction)), 1));
}

TEST(TestQueue, ConcurrencyTest) {
  LockFreeQueue<transaction::SerializableTransaction> queue;
  const auto item_size = sizeof(transaction::SerializableTransaction);

  auto producer = std::thread([&]() {
    for (auto idx = 0; idx < NO_OPERATIONS; idx++) { queue.Push(transaction::Transaction()); }
  });

  auto consumer = std::thread([&]() {
    auto idx = 0UL;
    for (; idx < NO_OPERATIONS; idx++) {
      auto n_items = 0;
      while (n_items == 0) { n_items = queue.SizeApprox(); }
      queue.LoopElement(1, [&](auto &txn) {
        EXPECT_EQ(txn.state, transaction::Transaction::State::IDLE);
        EXPECT_EQ(txn.commit_ts, 0);
        EXPECT_EQ(txn.max_observed_gsn, 0);
        return true;
      });
      EXPECT_TRUE((queue.last_loop_bytes == item_size) || (queue.last_loop_bytes == item_size * 2));
      queue.Erase(1);
    }
  });

  producer.join();
  consumer.join();
  EXPECT_EQ(queue.head_.load(), queue.tail_.load() >> 32);
  EXPECT_EQ(queue.tail_.load() & queue.SIZE_MASK, 0);
}

}  // namespace leanstore

auto main(int argc, char **argv) -> int {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}