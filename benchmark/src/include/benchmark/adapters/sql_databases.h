#include "benchmark/adapters/adapter.h"

#include "cppconn/driver.h"
#include "cppconn/prepared_statement.h"
#include "cppconn/statement.h"
#include "pqxx/pqxx"
#include "share_headers/config.h"
#include "sqlite_cpp/sqlite_modern_cpp.h"

#include <atomic>
#include <thread>

struct BaseDatabase {
  std::atomic<uint64_t> total_txn_completed = 0;

  virtual ~BaseDatabase() = default;

  auto StartProfilingThread(const std::string &system_name, std::atomic<bool> &keep_running,
                            std::atomic<uint64_t> &completed_txn) -> std::thread;
};

struct SQLiteDB : BaseDatabase {
  std::string db_path;
  sqlite::database ui;

  explicit SQLiteDB(const std::string &path);
  ~SQLiteDB() override = default;

  void StartTransaction(bool serializable = false);
  void CommitTransaction();
  auto DatabaseSize() -> float;
};

struct PostgresDB : BaseDatabase {
  pqxx::connection *conn;
  inline static thread_local std::unique_ptr<pqxx::transaction_base> txn = nullptr;

  PostgresDB();
  ~PostgresDB() override;

  void StartTransaction(bool si = false);
  void CommitTransaction();
  auto DatabaseSize() -> float;
};

struct MySQLDB : BaseDatabase {
  std::string db_conn;
  inline static thread_local std::unique_ptr<sql::Connection> conn = nullptr;

  MySQLDB();
  ~MySQLDB() override = default;

  void PrepareThread();
  void StartTransaction();
  void CommitTransaction();
  auto DatabaseSize() -> float;
};
