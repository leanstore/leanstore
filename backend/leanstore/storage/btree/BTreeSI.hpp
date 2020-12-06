inline u8 myWorkerID()
{
   return cr::Worker::my().worker_id;
}
inline u64 myTTS()
{
   return cr::Worker::my().active_tx.tts;
}
inline bool isVisibleForMe(u8 worker_id, u64 tts)
{
   return cr::Worker::my().isVisibleForMe(worker_id, tts);
}
inline bool isVisibleForMe(u64 wtts)
{
   return cr::Worker::my().isVisibleForMe(wtts);
}
static void undo(void* btree_object, const u8* wal_entry_ptr, const u64 tts);
static void todo(void* btree_object, const u8* wal_entry_ptr, const u64 tts);
