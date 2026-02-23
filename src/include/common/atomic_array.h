#include <atomic>
#include <cstddef>
#include <memory>
#include <stdexcept>

namespace leanstore {

template <typename T>
class AtomicArray {
  static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable for std::atomic");

  size_t size_;
  std::unique_ptr<std::atomic<T>[]> data_;

 public:
  // Constructor: allocate array of size n, default-initialized
  explicit AtomicArray(size_t n) : size_(n), data_(std::make_unique<std::atomic<T>[]>(n)) {}

  // Access element (non-const)
  inline auto operator[](size_t i) -> std::atomic<T> & {
    if (i >= size_) { throw std::out_of_range("AtomicArray index out of range"); }
    return data_[i];
  }

  size_t Size() const { return size_; }

  auto Min(std::memory_order order = std::memory_order_relaxed) const -> T {
    if (size_ == 0) { throw std::runtime_error("Cannot compute min of empty array"); }

    auto min_val = data_[0].load(order);
    for (auto i = 1U; i < size_; i++) {
      auto val = data_[i].load(order);
      if (val < min_val) { min_val = val; }
    }
    return min_val;
  }

  // Disable copy/move
  AtomicArray(const AtomicArray &)            = delete;
  AtomicArray &operator=(const AtomicArray &) = delete;
  AtomicArray(AtomicArray &&)                 = delete;
  AtomicArray &operator=(AtomicArray &&)      = delete;
};

}  // namespace leanstore