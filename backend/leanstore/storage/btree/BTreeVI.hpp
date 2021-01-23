inline SwipType sizeToVT(u64 size)
{
   return SwipType(reinterpret_cast<BufferFrame*>(size));
}
void iterateDescVI(u8* start_key, u16 key_length, function<bool(HybridPageGuard<BTreeNode>& guard, s16 pos)> callback);
OP_RESULT lookupVI(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback);
OP_RESULT insertVI(u8* key, u16 key_length, u16 valueLength, u8* value);
OP_RESULT updateVI(u8* key, u16 key_length, function<void(u8* value, u16 value_size)>, WALUpdateGenerator = {{}, {}, 0});
OP_RESULT removeVI(u8* key, u16 key_length);
void scanDescVI(u8* start_key, u16 key_length, function<bool(u8* key, u16 key_length, u8* value, u16 value_length)>, function<void()>);
void scanAscVI(u8* start_key, u16 key_length, function<bool(u8* key, u16 key_length, u8* value, u16 value_length)>, function<void()>);
static void undoVI(void* btree_object, const u8* wal_entry_ptr, const u64 tts);
static void applyDeltaVI(u8* dst, u8* delta, u16 delta_size);
