#pragma once

#include "common/typedefs.h"

namespace leanstore {

static constexpr pageid_t METADATA_PAGE_ID = 0;
static constexpr u8 SAMPLING_SIZE          = 16;
// -------------------------------------------------------------------------------------
static constexpr u64 PAGE_SIZE = 4096;
static constexpr u64 KB        = 1024ULL;
static constexpr u64 MB        = 1024ULL * 1024;
static constexpr u64 GB        = 1024ULL * 1024 * 1024;
// -------------------------------------------------------------------------------------
static constexpr u64 MSB       = static_cast<u64>(1) << 63;
static constexpr u64 MSB_MASK  = ~(MSB);
static constexpr u64 MSB2      = static_cast<u64>(1) << 62;
static constexpr u64 MSB2_MASK = ~(MSB2);
// -------------------------------------------------------------------------------------
static constexpr u16 MAX_NUMBER_OF_WORKER = 256;
// -------------------------------------------------------------------------------------
static constexpr u32 BLK_BLOCK_SIZE     = 4096;
static constexpr u32 CPU_CACHELINE_SIZE = 64;

}  // namespace leanstore