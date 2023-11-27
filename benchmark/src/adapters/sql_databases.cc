#include "benchmark/adapters/sql_databases.h"

#include "mysql_driver.h"

auto BaseDatabase::StartProfilingThread(const std::string &system_name, std::atomic<bool> &keep_running,
                                        std::atomic<uint64_t> &completed_txn) -> std::thread {
  return std::thread([&]() {
    uint64_t cnt = 0;
    std::printf("system,ts,tx\n");
    while (keep_running.load()) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      auto progress = completed_txn.exchange(0);
      total_txn_completed += progress;
      std::printf("%s,%lu,%lu\n", system_name.c_str(), cnt++, progress);
    }
    std::printf("Halt Profiling thread\n");
  });
}

// -------------------------------------------------------------------------------------
SQLiteDB::SQLiteDB(const std::string &path) : db_path(path), ui(path) {
  ui << "PRAGMA journal_mode = WAL";
  ui << "PRAGMA synchronous = NORMAL";
  ui << "PRAGMA read_uncommitted = true;";
  ui << "PRAGMA page_size = 4096";
  ui << "PRAGMA cache_size = 8388608;";
}

void SQLiteDB::StartTransaction(bool serializable) {
  if (serializable) {
    ui << "BEGIN IMMEDIATE;";
  } else {
    ui << "BEGIN";
  }
}

void SQLiteDB::CommitTransaction() { ui << "COMMIT"; }

auto SQLiteDB::DatabaseSize() -> float {
  uint64_t page_cnt;
  ui << "PRAGMA page_count" >> page_cnt;
  return static_cast<float>(page_cnt * 4096) / (1024 * 1024 * 1024);
}

// -------------------------------------------------------------------------------------
/**
 * Remember to disable peer authentication for Postgres user
 */
PostgresDB::PostgresDB() {
  const auto db_conn =
    "host=/var/run/postgresql "
    "dbname=postgres "
    "user=postgres "
    "port=5432 "
    "sslmode=disable";
  conn = new pqxx::connection{db_conn};
}

PostgresDB::~PostgresDB() { delete conn; }

void PostgresDB::StartTransaction(bool si) {
  assert(txn == nullptr);
  if (si) {
    txn = std::make_unique<pqxx::transaction<pqxx::repeatable_read>>(*conn);
  } else {
    txn = std::make_unique<pqxx::transaction<pqxx::read_committed>>(*conn);
  }
}

void PostgresDB::CommitTransaction() {
  assert(txn != nullptr);
  txn->commit();
  txn = nullptr;
}

auto PostgresDB::DatabaseSize() -> float {
  pqxx::work tx{*conn};
  auto db_size = tx.query_value<uint64_t>("SELECT pg_database_size('postgres');");
  return static_cast<float>(db_size) / (1024 * 1024 * 1024);
}

// -------------------------------------------------------------------------------------
MySQLDB::MySQLDB() { db_conn = "leanstore:leanstore@unix(/var/run/mysqld/mysqld.sock)/test?charset=utf8"; }

void MySQLDB::PrepareThread() {
  conn.reset(get_driver_instance()->connect("unix:///var/run/mysqld/mysqld.sock", "leanstore", "leanstore"));
  conn->setSchema("test");
  std::unique_ptr<sql::Statement> stmt(conn->createStatement());
  if (FLAGS_txn_default_isolation_level == "rc") {
    stmt->execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
  } else if (FLAGS_txn_default_isolation_level == "si") {
    stmt->execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
  } else if (FLAGS_txn_default_isolation_level == "ser") {
    stmt->execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;");
  } else {
    stmt->execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;");
  }
}

void MySQLDB::StartTransaction() {
  std::unique_ptr<sql::Statement> stmt(conn->createStatement());
  stmt->execute("START TRANSACTION;");
}

void MySQLDB::CommitTransaction() {
  std::unique_ptr<sql::Statement> stmt(conn->createStatement());
  stmt->execute("COMMIT;");
}

auto MySQLDB::DatabaseSize() -> float {
  std::unique_ptr<sql::Statement> stmt(conn->createStatement());
  auto res = stmt->executeQuery(
    "SELECT ALLOCATED_SIZE FROM INFORMATION_SCHEMA.INNODB_TABLESPACES WHERE NAME='test/YCSB_TABLE';");
  assert(res->rowsCount() == 1);
  [[maybe_unused]] auto flag = res->next();
  assert(flag);
  return res->getDouble("ALLOCATED_SIZE") / (1024 * 1024 * 1024);
}
