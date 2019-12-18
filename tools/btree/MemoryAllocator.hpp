#include "Primitives.hpp"
namespace libgcc
{
static u8* memory_block;
static atomic<u64> memory_head;
}