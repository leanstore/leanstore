#include "benchmark/adapters/sql_databases.h"

SQLiteDB::SQLiteDB(const std::string &path) : db_path(path), ui(path) {
  ui << "PRAGMA journal_mode = WAL";
  ui << "PRAGMA synchronous = NORMAL";
  ui << "PRAGMA read_uncommitted = true;";
  ui << "PRAGMA page_size = 4096";
  auto cache_size =
    std::string("PRAGMA cache_size = -") + std::to_string(FLAGS_bm_physical_gb * 1024 * 1024 * 1024) + ";";
  ui << cache_size;
}

void SQLiteDB::StartTransaction(bool serializable) {
  while (true) {
    try {
      if (serializable) {
        ui << "BEGIN IMMEDIATE;";
      } else {
        ui << "BEGIN";
      }
      return;
    } catch (...) {}
  }
}

void SQLiteDB::CommitTransaction() { ui << "COMMIT"; }

auto SQLiteDB::DatabaseSize() -> float {
  uint64_t page_cnt;
  ui << "PRAGMA page_count" >> page_cnt;
  return static_cast<float>(page_cnt * 4096) / (1024 * 1024 * 1024);
}
