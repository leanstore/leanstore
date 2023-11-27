#include "storage/btree/locking.h"
#include "storage/btree/node.h"
#include "storage/page.h"

namespace leanstore::storage {

template <class T>
GuardO<T>::GuardO(buffer::BufferManager *buffer, pageid_t pid)
    : sync::OptimisticGuard<T>(pid, reinterpret_cast<T *>(buffer->ToPtr(pid)), &(buffer->GetPageState(pid)),
                               [buffer](pageid_t pid) { buffer->HandlePageFault(pid); }) {}

template <class T>
template <class T2>
GuardO<T>::GuardO(buffer::BufferManager *buffer, pageid_t pid, GuardO<T2> &parent) {
  // Note, parent's OptimisticGuard multiple times,
  //  so we shouldn't MOVED that guard
  parent.ValidateOrRestart(false);
  // We need to validate parent first to guarantee we retrieve the correct pid
  sync::OptimisticGuard<T>::page_id_          = pid;
  sync::OptimisticGuard<T>::page_ptr_         = reinterpret_cast<T *>(buffer->ToPtr(pid));
  sync::OptimisticGuard<T>::ps_               = &(buffer->GetPageState(pid));
  sync::OptimisticGuard<T>::mode_             = sync::GuardMode::OPTIMISTIC;
  sync::OptimisticGuard<T>::page_loader_func_ = [buffer](pageid_t pid) { buffer->HandlePageFault(pid); };
  sync::OptimisticGuard<T>::Init();
}

template <class T>
GuardS<T>::GuardS(buffer::BufferManager *buffer, pageid_t pid)
    : sync::SharedGuard<T>(pid, reinterpret_cast<T *>(buffer->FixShare(pid)), &(buffer->GetPageState(pid))){};

template <class T>
GuardS<T>::GuardS(GuardO<T> &&op_guard) : sync::SharedGuard<T>(static_cast<sync::OptimisticGuard<T> &&>(op_guard)) {}

template <class T>
GuardX<T>::GuardX(buffer::BufferManager *buffer, pageid_t pid)
    : sync::ExclusiveGuard<T>(pid, reinterpret_cast<T *>(buffer->FixExclusive(pid)), &(buffer->GetPageState(pid))) {}

template <class T>
GuardX<T>::GuardX(buffer::BufferManager *buffer, Page *alloc_page)
    : sync::ExclusiveGuard<T>(buffer->ToPID(alloc_page), reinterpret_cast<T *>(alloc_page),
                              &(buffer->GetPageState(buffer->ToPID(alloc_page)))) {}

template <class T>
GuardX<T>::GuardX(GuardO<T> &&op_guard) : sync::ExclusiveGuard<T>(static_cast<sync::OptimisticGuard<T> &&>(op_guard)) {}

template struct GuardO<MetadataPage>;
template struct GuardO<BTreeNode>;
template struct GuardX<MetadataPage>;
template struct GuardX<BTreeNode>;
template struct GuardS<MetadataPage>;
template struct GuardS<BTreeNode>;

// Optimistic Lock-Coupling facility
template GuardO<BTreeNode>::GuardO(buffer::BufferManager *, pageid_t, GuardO<MetadataPage> &);
template GuardO<BTreeNode>::GuardO(buffer::BufferManager *, pageid_t, GuardO<BTreeNode> &);

}  // namespace leanstore::storage