#pragma once
#include <string.h>
#include <algorithm>
#include <atomic>
#include <cassert>
#include <iostream>
#include <vector>

enum class PageType : uint8_t { BTreeInner = 1, BTreeLeaf = 2 };
