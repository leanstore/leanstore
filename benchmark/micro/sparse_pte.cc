#include "storage/extent/extent_list.h"

#include "share_headers/logger.h"

#include <sys/mman.h>

using ExtentList = leanstore::storage::ExtentList;

#define PAGE_SIZE 4096

int main() {
  auto storage_size      = 1ULL << 30;  // 512GB
  auto tier_virtual_size = ExtentList::NO_TIERS * storage_size;
  auto bm                = reinterpret_cast<u8 *>(
    mmap(nullptr, tier_virtual_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
  Ensure(bm != MAP_FAILED);
  std::array<u64, ExtentList::NO_TIERS> start_addr;
  for (auto tier = 0; tier < ExtentList::NO_TIERS; tier++) { start_addr[tier] = storage_size * tier; }
  LOG_INFO("Page table size %s - tier vitr size %llu", leanstore::PageTableSize().c_str(), tier_virtual_size);

  for (auto idx = 0ULL; idx < tier_virtual_size; idx++) { bm[idx] = 1; }
  LOG_INFO("Page table size %s", leanstore::PageTableSize().c_str());

  // Allocate 10000 BLOBs
  for (int blob = 0; blob <= 10000; blob++) {
    for (auto tier = 0; tier < 20; tier++) {
      auto addr = start_addr[tier] + blob * PAGE_SIZE * ExtentList::ExtentSize(tier);
      if (addr < tier_virtual_size) { bm[addr] = 1; }
    }
    if (blob % 1000 == 0) { LOG_INFO("After Blob %d, Page table size %s", blob, leanstore::PageTableSize().c_str()); }
  }

  // Now delete ~90% of all BLOBs
  LOG_INFO("De-allocation");
  for (int blob = 0; blob <= 10000; blob++) {
    if (rand() % 10 < 9) {
      for (auto tier = 0; tier < ExtentList::NO_TIERS; tier++) {
        auto addr = start_addr[tier] + blob * PAGE_SIZE * ExtentList::ExtentSize(tier);
        if (addr < tier_virtual_size) {
          int ret = madvise(&bm[addr], PAGE_SIZE, MADV_DONTNEED);
          Ensure(ret >= 0);
        }
      }
    }
    if (blob % 1000 == 0) { LOG_INFO("After Blob %d, Page table size %s", blob, leanstore::PageTableSize().c_str()); }
  }
}