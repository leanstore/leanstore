#include "JumpMU.hpp"

#include <signal.h>
// -------------------------------------------------------------------------------------
namespace jumpmu
{
__thread int checkpoint_counter = 0;
__thread jmp_buf env[JUMPMU_STACK_SIZE];
__thread int checkpoint_stacks_counter[JUMPMU_STACK_SIZE];
__thread void (*de_stack_arr[JUMPMU_STACK_SIZE])(void*);
__thread void* de_stack_obj[JUMPMU_STACK_SIZE];
__thread int de_stack_counter = 0;
__thread bool in_jump = false;
void jump()
{
   assert(checkpoint_counter > 0);
   assert(de_stack_counter >= 0);
   auto c_c_stacks_counter = checkpoint_stacks_counter[checkpoint_counter - 1];
   if (de_stack_counter > c_c_stacks_counter) {
      int begin = de_stack_counter - 1;  // inc
      int till = c_c_stacks_counter;     // inc
      assert(begin >= 0);
      assert(till >= 0);
      in_jump = true;
      for (int i = begin; i >= till; i--) {
         de_stack_arr[i](de_stack_obj[i]);
      }
      in_jump = false;
   }
   auto& env_to_jump = jumpmu::env[jumpmu::checkpoint_counter - 1];
   checkpoint_counter--;
   longjmp(env_to_jump, 1);
}
}  // namespace jumpmu
