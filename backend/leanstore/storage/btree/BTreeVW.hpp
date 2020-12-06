OP_RESULT lookupVW(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback);
OP_RESULT insertVW(u8* key, u16 key_length, u16 valueLength, u8* value);
OP_RESULT updateVW(u8* key, u16 key_length, function<void(u8* value, u16 value_size)>, WALUpdateGenerator = {{}, {}, 0});
OP_RESULT removeVW(u8* key, u16 key_length);
OP_RESULT scanAscVW(u8* start_key,
                    u16 key_length,
                    function<bool(u8* key, u16 key_length, u8* value, u16 value_length)> callback,
                    function<void()> undo);
// starts at the key + 1
OP_RESULT scanDescVW(u8* start_key,
                     u16 key_length,
                     function<bool(u8* key, u16 key_length, u8* value, u16 value_length)> callback,
                     function<void()> undo);
bool reconstructTupleVW(u8* payload, u16& payload_length, u8 worker_id, u64 lsn, u32 in_memory_offset);
static void applyDeltaVW(u8* dst, u16 dst_size, const u8* delta, u16 delta_size);
static void undoVW(void* btree_object, const u8* wal_entry_ptr, const u64 tts);
static void todoVW(void* btree_object, const u8* wal_entry_ptr, const u64 tts);
