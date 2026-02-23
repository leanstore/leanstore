#include "benchmark/adapters/adapter.h"
#include "share_headers/config.h"
#include "sqlite_cpp/sqlite_modern_cpp.h"

#include <atomic>
#include <thread>

struct SQLiteDB : BaseDatabase {
  std::string db_path;
  sqlite::database ui;

  explicit SQLiteDB(const std::string &path);
  ~SQLiteDB() override = default;

  void StartTransaction(bool serializable = false);
  void CommitTransaction();
  auto DatabaseSize() -> float;
};
