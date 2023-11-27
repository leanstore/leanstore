#pragma once

#include "buffer/buffer_manager.h"
#include "common/typedefs.h"
#include "sync/page_guard/exclusive_guard.h"
#include "sync/page_guard/optimistic_guard.h"
#include "sync/page_guard/shared_guard.h"

namespace leanstore::storage {

/* Aliases to quickly (and easily) access the PageGuard for BTree */
template <class T>
struct GuardO : public sync::OptimisticGuard<T> {
  GuardO(buffer::BufferManager *buffer, pageid_t pid);
  template <class T2>
  GuardO(buffer::BufferManager *buffer, pageid_t pid, GuardO<T2> &parent);
};

template <class T>
struct GuardS : public sync::SharedGuard<T> {
  GuardS(buffer::BufferManager *buffer, pageid_t pid);
  explicit GuardS(GuardO<T> &&op_guard);
};

template <class T>
struct GuardX : public sync::ExclusiveGuard<T> {
  GuardX(buffer::BufferManager *buffer, pageid_t pid);
  // for new allocated page. This page should already be X-locked
  GuardX(buffer::BufferManager *buffer, Page *alloc_page);
  explicit GuardX(GuardO<T> &&op_guard);
};

}  // namespace leanstore::storage