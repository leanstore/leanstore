#pragma once
#include "Exceptions.hpp"
#include "Types.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/storage/btree/core/WALMacros.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
#include <cassert>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
// -------------------------------------------------------------------------------------
// Helpers to generate a descriptor that describes which attributes are in-place updating in a fixed-size value
#define UpdateDescriptorInit(Name, Count)                                                                                                     \
   u8 Name##_buffer[sizeof(leanstore::UpdateSameSizeInPlaceDescriptor) + (sizeof(leanstore::UpdateSameSizeInPlaceDescriptor::Slot) * Count)]; \
   auto& Name = *reinterpret_cast<leanstore::UpdateSameSizeInPlaceDescriptor*>(Name##_buffer);                                                \
   Name.count = Count;

#define UpdateDescriptorFillSlot(Name, Index, Type, Attribute) \
   Name.slots[Index].offset = offsetof(Type, Attribute);       \
   Name.slots[Index].length = sizeof(Type::Attribute);

#define UpdateDescriptorGenerator1(Name, Type, A0) \
   UpdateDescriptorInit(Name, 1);                  \
   UpdateDescriptorFillSlot(Name, 0, Type, A0);

#define UpdateDescriptorGenerator2(Name, Type, A0, A1) \
   UpdateDescriptorInit(Name, 2);                      \
   UpdateDescriptorFillSlot(Name, 0, Type, A0);        \
   UpdateDescriptorFillSlot(Name, 1, Type, A1);

#define UpdateDescriptorGenerator3(Name, Type, A0, A1, A2) \
   UpdateDescriptorInit(Name, 3);                          \
   UpdateDescriptorFillSlot(Name, 0, Type, A0);            \
   UpdateDescriptorFillSlot(Name, 1, Type, A1);            \
   UpdateDescriptorFillSlot(Name, 2, Type, A2);

#define UpdateDescriptorGenerator4(Name, Type, A0, A1, A2, A3) \
   UpdateDescriptorInit(Name, 4);                              \
   UpdateDescriptorFillSlot(Name, 0, Type, A0);                \
   UpdateDescriptorFillSlot(Name, 1, Type, A1);                \
   UpdateDescriptorFillSlot(Name, 2, Type, A2);                \
   UpdateDescriptorFillSlot(Name, 3, Type, A3);

// -------------------------------------------------------------------------------------
// Unified interface used by our benchmarks for different storage engines including LeanStore
template <class Record>
class Adapter
{
  public:
   // Scan in ascending order, scan can fail if it is executed in optimistic mode without latching the leaves
   virtual void scan(const typename Record::Key& key,
                     const std::function<bool(const typename Record::Key&, const Record&)>& found_record_cb,
                     std::function<void()> reset_if_scan_failed_cb) = 0;
   // -------------------------------------------------------------------------------------
   virtual void scanDesc(const typename Record::Key& key,
                         const std::function<bool(const typename Record::Key&, const Record&)>& found_record_cb,
                         std::function<void()> reset_if_scan_failed_cb) = 0;
   // -------------------------------------------------------------------------------------
   virtual void insert(const typename Record::Key& key, const Record& record) = 0;
   // -------------------------------------------------------------------------------------
   virtual void lookup1(const typename Record::Key& key, const std::function<void(const Record&)>& callback) = 0;
   // -------------------------------------------------------------------------------------
   virtual void update1(const typename Record::Key& key,
                        const std::function<void(Record&)>& update_the_record_in_place_cb,
                        leanstore::UpdateSameSizeInPlaceDescriptor& update_descriptor) = 0;
   // -------------------------------------------------------------------------------------
   // Returns false if the record was not found
   virtual bool erase(const typename Record::Key& key) = 0;
   // -------------------------------------------------------------------------------------
   template <class Field>
   Field lookupField(const typename Record::Key& key, Field Record::*f)
   {
      UNREACHABLE();
   }
};
