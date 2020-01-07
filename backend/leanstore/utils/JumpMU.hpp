#pragma once
#include <setjmp.h>
#include <cassert>
#include <signal.h>
#include <utility>

#define JUMPMU_STACK_SIZE 20
namespace jumpmu
{
extern __thread int checkpoint_counter;
extern __thread jmp_buf env[JUMPMU_STACK_SIZE];
extern __thread int val[JUMPMU_STACK_SIZE];
extern __thread int checkpoint_stacks_counter[JUMPMU_STACK_SIZE];
extern __thread void (*de_stack_arr[JUMPMU_STACK_SIZE])(void*);
extern __thread void* de_stack_obj[JUMPMU_STACK_SIZE];
extern __thread int de_stack_counter;
void restore();
inline void decrement()
{
  de_stack_obj[de_stack_counter - 1] = nullptr;
  de_stack_arr[de_stack_counter - 1] = nullptr;
  de_stack_counter--;
  assert(de_stack_counter >= 0);
}

}  // namespace jumpmu
// -------------------------------------------------------------------------------------
#define jumpmu_registerDestructor()                       \
  assert(jumpmu::de_stack_counter < JUMPMU_STACK_SIZE);   \
  assert(jumpmu::checkpoint_counter < JUMPMU_STACK_SIZE); \
  jumpmu::de_stack_arr[jumpmu::de_stack_counter] = &des;  \
  jumpmu::de_stack_obj[jumpmu::de_stack_counter] = this;  \
  jumpmu::de_stack_counter++;

#define jumpmu_defineCustomDestructor(NAME) \
  static void des(void* t) { reinterpret_cast<NAME*>(t)->~NAME(); }

#define jumpmu_return           \
  jumpmu::checkpoint_counter--; \
  return

#define jumpmu_continue         \
  jumpmu::checkpoint_counter--; \
  continue

// ATTENTION DO NOT DO ANYTHING BETWEEN setjmp and if !!
#define jumpmuTry()                                                                         \
  assert(jumpmu::de_stack_counter >= 0);                                                    \
  jumpmu::checkpoint_stacks_counter[jumpmu::checkpoint_counter] = jumpmu::de_stack_counter; \
  int _lval = setjmp(jumpmu::env[jumpmu::checkpoint_counter++]);                            \
  if (_lval == 0) {
#define jumpmuCatch()           \
  jumpmu::checkpoint_counter--; \
  }                             \
  else

template <typename T>
class JMUW
{
 public:
  T obj;
  template <typename... Args>
  JMUW(Args&&... args) : obj(std::forward<Args>(args)...)
  {
    jumpmu_registerDestructor();
  }
  static void des(void* t) { reinterpret_cast<JMUW<T>*>(t)->~JMUW<T>(); }
  ~JMUW() {
    const auto before = jumpmu::de_stack_counter;
    jumpmu::decrement();
    const auto after = jumpmu::de_stack_counter;
    assert(before - 1 == after);
  }
  T* operator->() { return reinterpret_cast<T*>(&obj); }
};
