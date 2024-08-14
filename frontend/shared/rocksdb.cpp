#include "rocksdb/db.h"

using namespace rocksdb;

// Force the compiler to include type information for rocksdb::DB
void ensureTypeInfo() {
    const std::type_info& ti = typeid(DB);
}