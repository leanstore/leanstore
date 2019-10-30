#pragma once
#include <atomic>
#include <unistd.h>

using namespace std;
namespace optimistic {

struct NodeBase {
   PageType type;
   uint16_t count;
   atomic<uint64_t> version;
   NodeBase() : version(8) {}
};

using Node = NodeBase;

uint64_t readLockOrRestart(Node &node);
bool checkOrRestart(Node &n, uint64_t v);
bool readUnlockOrRestart(Node &n, uint64_t v);
bool readUnlockOrRestart(Node &n, uint64_t v, NodeBase &locked_node);
bool upgradeToWriteLockOrRestart(Node &n, uint64_t v);
bool upgradeToWriteLockOrRestart(Node &n, uint64_t v, Node locked_node);
void writeLockOrRestart(Node &n);
void writeUnlock(Node &n);
void writeUnlockObsolete(Node &n);
uint64_t awaitNodeUnlocked(Node &n);
uint64_t setLockedBit(uint64_t v);
bool isObsolete(uint64_t v);

#define restart() return 0

uint64_t readLockOrRestart(Node &node)
{
    uint64_t v = awaitNodeUnlocked(node);
    if (isObsolete(v)) {
        restart();
    }
    return v;
}
bool checkOrRestart(Node &n, uint64_t v)
{
    return readUnlockOrRestart(n, v);
}
bool readUnlockOrRestart(Node &n, uint64_t v)
{
    if (v != n.version.load()) {
        restart();
    }
    return true;
}
bool readUnlockOrRestart(Node &n, uint64_t v, NodeBase &locked_node)
{
    if (v != n.version.load()) {
        writeUnlock(locked_node);
        restart();
    }
    return true;
}
bool upgradeToWriteLockOrRestart(Node &n, uint64_t v)
{
    if (!std::atomic_compare_exchange_strong(&n.version, &v, setLockedBit(v))) {
        restart();
    }
    return true;
}

bool upgradeToWriteLockOrRestart(Node &n, uint64_t v, Node locked_node)
{
    if (!std::atomic_compare_exchange_strong(&n.version, &v, setLockedBit(v))) {
        writeUnlock(locked_node);
        restart();
    }
    return true;
}
void writeLockOrRestart(Node &n)
{
    uint64_t v;
    do {
        v = readLockOrRestart(n);
    } while (!upgradeToWriteLockOrRestart(n, v));
}
void writeUnlock(Node &n)
{
    n.version.fetch_add(2);
}
void writeUnlockObsolete(Node &n)
{
    n.version.fetch_add(3);
}

// Helper functions
uint64_t awaitNodeUnlocked(Node &n)
{
    uint64_t v = n.version.load();

    while ((v & 2) == 2) { //spin bf_s_lock
        //usleep(5);
        v = n.version.load();
    }
    return v;
}
uint64_t setLockedBit(uint64_t v)
{
    return v + 2;
}
bool isObsolete(uint64_t v)
{
    return (v & 1) == 1;
}
}
