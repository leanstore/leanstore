#pragma once
// -------------------------------------------------------------------------------------
#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <memory>
#include <queue>
#include <vector>
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
using MessageFunction = void (*)(void* pollerEnv, uintptr_t userData);
// using MessageFunction = void(*)(void* pollerEnv, uintptr_t userData, ...);
struct QueueMessageFrame {
   int toId;
   MessageFunction fun_ptr;
   uintptr_t user_data;
   QueueMessageFrame(int toId, MessageFunction fun, uintptr_t user_data) : toId(toId), fun_ptr(fun), user_data(user_data){};
};
struct Message {
   std::atomic<MessageFunction> fun_ptr;
   std::atomic<uintptr_t> user_data;
};
struct Postbox {
   static constexpr int postbox_size_byte = 128;
   static constexpr int max_messages_count = 7;
   Message messages[max_messages_count];
   std::atomic<uint64_t> toggle_bit;
   static constexpr int padding_size = (postbox_size_byte - sizeof(toggle_bit) - sizeof(messages));
   char padding[padding_size];
};
static_assert(sizeof(Postbox) == Postbox::postbox_size_byte);
// -------------------------------------------------------------------------------------
class MessageHandlerManager;
class MessageHandler
{
   MessageHandlerManager* manager;
   int selfId = -1;
   int numberThreads = -1;
   Postbox* postbox;
   int outstandingSends = 0;
   std::vector<std::queue<QueueMessageFrame>> sendQueue;

  public:
   // -------------------------------------------------------------------------------------
   MessageHandler();
   MessageHandler(MessageHandlerManager* manager, int numberThreads, int selfId);
   // -------------------------------------------------------------------------------------
   ~MessageHandler();
   MessageHandler(const MessageHandler& other) = delete;
   explicit MessageHandler(MessageHandler&& other);
   MessageHandler& operator=(const MessageHandler& other) = delete;
   MessageHandler& operator=(MessageHandler&& other) = delete;
   // -------------------------------------------------------------------------------------
   friend void swap(MessageHandler& first, MessageHandler& second)
   {
      using std::swap;
      swap(first.manager, second.manager);
      swap(first.selfId, second.selfId);
      swap(first.numberThreads, second.numberThreads);
      swap(first.outstandingSends, second.outstandingSends);
      swap(first.sendQueue, second.sendQueue);
      swap(first.postbox, second.postbox);
   }
   // -------------------------------------------------------------------------------------
   int poll(void* pollerEnv, int max = 0);  // incoming messages, execute functions
   bool sendMessage(int toId, MessageFunction fun, uintptr_t userData);
   int getSelfId();
   // -------------------------------------------------------------------------------------
   void checkAllOutstandingMessageBox();
};
// -------------------------------------------------------------------------------------
class MessageHandlerManager
{
   friend class MessageHandler;
   std::vector<MessageHandler> handlers;

  public:
   MessageHandlerManager(int number);
   // -------------------------------------------------------------------------------------
   MessageHandlerManager(const MessageHandlerManager&) = delete;
   MessageHandlerManager(MessageHandlerManager&&) = delete;
   MessageHandlerManager& operator=(const MessageHandlerManager&) = delete;
   MessageHandlerManager& operator=(MessageHandlerManager&&) = delete;
   // -------------------------------------------------------------------------------------
   MessageHandler& getMessageHandler(int id);
   bool dbgSendMessage(int fromId, int toId, MessageFunction fun, uintptr_t userData);
};
// -------------------------------------------------------------------------------------
}  // namespace mean
// -------------------------------------------------------------------------------------
