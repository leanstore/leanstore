#pragma once

#include "benchmark/utils/shared_schema.h"
#include "leanstore/leanstore.h"

#include <cstring>

namespace gitclone {

struct Operator {
  enum class Type { OPEN, FSTAT, CLOSE, READ, WRITE };

  inline static std::unordered_map<std::string, Type> type_resolver = {{"SYS_openat", Operator::Type::OPEN},
                                                                       {"SYS_newfstatat", Operator::Type::FSTAT},
                                                                       {"SYS_close", Operator::Type::CLOSE},
                                                                       {"SYS_read", Operator::Type::READ},
                                                                       {"SYS_write", Operator::Type::WRITE}};

  Type op;
  int file_desc;
  size_t size;
  int flags;

  Operator(Type t, int fd, size_t sz, int flags) : op(t), file_desc(fd), size(sz), flags(flags) {}
};

using SystemFileRelation   = benchmark::FileRelation<0, 14588928>;
using TemplateFileRelation = benchmark::FileRelation<1, 4928>;
using ObjectFileRelation   = benchmark::FileRelation<2, 23944640>;

}  // namespace gitclone