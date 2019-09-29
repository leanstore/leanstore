#pragma once
#include <iostream>
#include <algorithm>
#include <atomic>
#include <cassert>
#include <string.h>
#include <vector>

enum class PageType : uint8_t {
   BTreeInner = 1,
   BTreeLeaf = 2
};
