#include "MessageHandler.hpp"
#include "Exceptions.hpp"
// -------------------------------------------------------------------------------------
#include <cstring>
// -------------------------------------------------------------------------------------
namespace mean
{
// -------------------------------------------------------------------------------------
MessageHandler::MessageHandler() : manager(nullptr), selfId(-1), numberThreads(-1), postbox(nullptr) {}
MessageHandler::MessageHandler(MessageHandlerManager* manager, int numberThreads, int selfId)
    : manager(manager),
      selfId(selfId),
      numberThreads(numberThreads),
      postbox((Postbox*)std::aligned_alloc(128, sizeof(Postbox) * numberThreads)),
      sendQueue(numberThreads)
{
   // std::memset(postbox.get(), 0, sizeof(Message)*numberThreads);
   for (int i = 0; i < numberThreads; i++) {
      auto& p = postbox[i];
      p.toggle_bit = 0;
   }
}
MessageHandler::~MessageHandler()
{
   std::free(postbox);
}
MessageHandler::MessageHandler(MessageHandler&& other) : MessageHandler()
{
   swap(*this, other);
   ensure(other.postbox == nullptr);
}

int MessageHandler::poll(void* pollerEnv, int max)
{
   int polled = 0;
   // poll incoming messages
   for (int i = 0; i < numberThreads; i++) {
      auto& p = postbox[i];
      auto incoming = p.toggle_bit.load();  // std::memory_order_relaxed); // TODO optimize
      if (incoming > 0) {
         ensure(incoming <= Postbox::max_messages_count);
         // std::cout << "incoming: " << incoming<< std::endl << std::flush;
         for (unsigned int i = 0; i < incoming; i++) {
            auto fun = p.messages[i].fun_ptr.load(std::memory_order_relaxed);
            auto userData = p.messages[i].user_data.load(std::memory_order_relaxed);
            (*fun)(pollerEnv, userData);
            polled++;
            if (polled > 0 && polled == max) {
               return polled;
            }
         }
         p.toggle_bit.store(0, std::memory_order_relaxed);
      }
   }
   checkAllOutstandingMessageBox();
   return polled;
}

int postToPostboxNoToggleBit(Postbox& p, std::queue<QueueMessageFrame>& q)
{
   int pushed = 0;
   while (q.size() > 0 && pushed < Postbox::max_messages_count) {
      auto& front = q.front();
      p.messages[pushed].fun_ptr.store(front.fun_ptr, std::memory_order_relaxed);
      p.messages[pushed].user_data.store(front.user_data, std::memory_order_relaxed);
      // std::cout << " indirect push: "  << pushed << " u1: " << front.user_data << " outs: " << std::endl << std::flush;
      q.pop();
      pushed++;
      // std::cout << "pushed: " << pushed << std::endl << std::flush;
   }
   return pushed;
}
void MessageHandler::checkAllOutstandingMessageBox()
{
   // check outstanding outbox
   if (outstandingSends > 0) {
      for (int toId = 0; toId < numberThreads; toId++) {
         auto& q = sendQueue[toId];
         auto& p = manager->handlers[toId].postbox[selfId];
         if (q.size() > 0 && p.toggle_bit.load() == 0) {
            int pushed = postToPostboxNoToggleBit(p, q);
            outstandingSends -= pushed;
            p.toggle_bit.store(pushed, std::memory_order_release);
            if (pushed > 0) {
               // std::cout << "[" << selfId << "] checkOutstandingMessageBox() toId: " << toId << "pushed: " << pushed << " outs: " <<
               // outstandingSends  << std::endl << std::flush;
            }
         }
      }
   }
}
/*
 * Either directly 'sends' a message or enqueues it in the sendQueue.
 * The message will then be sent with other messages the next time sentMessage is called for
 * the specific core id, or at the next poll call.
 */
bool MessageHandler::sendMessage(int toId, MessageFunction fun, uintptr_t userData)
{
   auto& p = manager->handlers[toId].postbox[selfId];
   if (p.toggle_bit.load(std::memory_order_relaxed) != 0) {
      // queue if sending is not possible
      sendQueue[toId].emplace(toId, fun, userData);
      outstandingSends++;
      // std::cout << "[" << selfId <<"] queued for " << toId <<  " outs: " << outstandingSends << " u1: " << userData << std::endl << std::flush;
      return false;
   }
   ensure(outstandingSends >= 0);
   int pushed = postToPostboxNoToggleBit(p, sendQueue[toId]);
   outstandingSends -= pushed;
   if (pushed < Postbox::max_messages_count) {
      // fast path, direct send
      p.messages[pushed].fun_ptr.store(fun, std::memory_order_relaxed);
      p.messages[pushed].user_data.store(userData, std::memory_order_relaxed);
      pushed++;
      // std::cout << "[" << selfId << "] direct push: "  << toId << " u1: " << userData << " outs: " << outstandingSends << std::endl << std::flush;
   } else {
      sendQueue[toId].emplace(toId, fun, userData);
      outstandingSends++;
      // std::cout << "[" << selfId <<"] queued for " << toId <<  " outs: " << outstandingSends << " u1: " << userData << std::endl << std::flush;
   }
   ensure(outstandingSends >= 0);
   // std::cout << "[" << selfId << "] toggle_bit "  << toId << " to: " << pushed << std::endl << std::flush;
   p.toggle_bit.store(pushed, std::memory_order_release);
   return true;
}
int MessageHandler::getSelfId()
{
   return selfId;
}
// -------------------------------------------------------------------------------------
MessageHandlerManager::MessageHandlerManager(int number)
{
   for (int i = 0; i < number; i++) {
      handlers.emplace_back(this, number, i);
   }
   // std::cout << "" << number << std::endl;
}
MessageHandler& MessageHandlerManager::getMessageHandler(int id)
{
   return handlers[id];
}
/*
 * WARNING: this function might lead to data races!!!
 * This can happen, if the thread using it is not the thread with the given fromId.
 * Means the message will show up for the Thread as it had been sent from itself.
 * (E.g. the message will land in the postbox of itselfs thread-id)
 */
bool MessageHandlerManager::dbgSendMessage(int fromId, int toId, MessageFunction fun, uintptr_t userData)
{
   return handlers[fromId].sendMessage(toId, fun, userData);
}
// -------------------------------------------------------------------------------------
}  // namespace mean
// ------------------------------------------------------------------------------------
