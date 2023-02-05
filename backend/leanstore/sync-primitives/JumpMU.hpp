#pragma once

#include "leanstore/utils/UserJumpReasons.hpp"

#include <setjmp.h>
#include <signal.h>

#include <cassert>
#include <utility>

#define JUMPMU_STACK_SIZE 20
namespace jumpmu
{
class JumpMUContext {
public:
  int checkpoint_counter = 0;
  jmp_buf env[JUMPMU_STACK_SIZE];
  int val[JUMPMU_STACK_SIZE];
  int checkpoint_stacks_counter[JUMPMU_STACK_SIZE];
  void (*de_stack_arr[JUMPMU_STACK_SIZE])(void*);
  void* de_stack_obj[JUMPMU_STACK_SIZE];
  int de_stack_counter = 0;
  bool in_jump;
  int user_jump_reason;
  // -------------------------------------------------------------------------------------
};
extern __thread JumpMUContext* thread_local_jumpmu_ctx;
inline void jump(leanstore::UserJumpReason jump_reason)
{
  auto& j = *thread_local_jumpmu_ctx;
  assert(j.checkpoint_counter > 0);
  assert(j.de_stack_counter >= 0);
	j.user_jump_reason = jump_reason;
  auto c_c_stacks_counter = j.checkpoint_stacks_counter[j.checkpoint_counter - 1];
  if (j.de_stack_counter > c_c_stacks_counter) {
    int begin = j.de_stack_counter - 1;  // inc
    int till = c_c_stacks_counter;     // inc
    assert(begin >= 0);
    assert(till >= 0);
    j.in_jump = true;
    for (int i = begin; i >= till; i--) {
      j.de_stack_arr[i](j.de_stack_obj[i]);
    }
    j.in_jump = false;
  }
  auto& env_to_jump = j.env[j.checkpoint_counter - 1];
  j.checkpoint_counter--;
  longjmp(env_to_jump, 1);
}
inline void jump()
{
	jump(leanstore::UserJumpReason::NoReason);
}
inline int user_jump_reason() {
  auto& j = *thread_local_jumpmu_ctx;
	return j.user_jump_reason;
}
inline void clearLastDestructor()
{
  auto& j = *thread_local_jumpmu_ctx;
  j.de_stack_obj[j.de_stack_counter - 1] = nullptr;
  j.de_stack_arr[j.de_stack_counter - 1] = nullptr;
  j.de_stack_counter--;
  assert(j.de_stack_counter >= 0);
}
}  // namespace jumpmu
   // -------------------------------------------------------------------------------------
   // clang-format off
#define jumpmu_registerDestructor()                       \
  assert(jumpmu::thread_local_jumpmu_ctx->de_stack_counter < JUMPMU_STACK_SIZE);assert(jumpmu::thread_local_jumpmu_ctx->checkpoint_counter < JUMPMU_STACK_SIZE);jumpmu::thread_local_jumpmu_ctx->de_stack_arr[jumpmu::thread_local_jumpmu_ctx->de_stack_counter] = &des;assert(jumpmu::thread_local_jumpmu_ctx->de_stack_arr[jumpmu::thread_local_jumpmu_ctx->de_stack_counter]!=nullptr);jumpmu::thread_local_jumpmu_ctx->de_stack_obj[jumpmu::thread_local_jumpmu_ctx->de_stack_counter] = this;jumpmu::thread_local_jumpmu_ctx->de_stack_counter++;

#define jumpmu_defineCustomDestructor(NAME) static void des(void* t) { reinterpret_cast<NAME*>(t)->~NAME(); }

// without calling destructors
#define jumpmu_return           \
  jumpmu::thread_local_jumpmu_ctx->checkpoint_counter--; return

#define jumpmu_break                            \
  jumpmu::thread_local_jumpmu_ctx->checkpoint_counter--; break

#define jumpmu_continue                         \
  jumpmu::thread_local_jumpmu_ctx->checkpoint_counter--; continue

// ATTENTION DO NOT DO ANYTHING BETWEEN setjmp and if !!
#define jumpmuTry()                                                                         \
  assert(jumpmu::thread_local_jumpmu_ctx->de_stack_counter >= 0); jumpmu::thread_local_jumpmu_ctx->checkpoint_stacks_counter[jumpmu::thread_local_jumpmu_ctx->checkpoint_counter] = jumpmu::thread_local_jumpmu_ctx->de_stack_counter; int _lval = setjmp(jumpmu::thread_local_jumpmu_ctx->env[jumpmu::thread_local_jumpmu_ctx->checkpoint_counter++]); if (_lval == 0) {

#define jumpmuCatch()           \
  jumpmu::thread_local_jumpmu_ctx->checkpoint_counter--; } else
   // clang-format on

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
  ~JMUW() { jumpmu::clearLastDestructor(); }
  T* operator->() { return reinterpret_cast<T*>(&obj); }
};
