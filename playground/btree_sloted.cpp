#include "BTreeSlotted.hpp"
// -------------------------------------------------------------------------------------
//template<class T>
//struct BTreeAdapter : public BTree {
//   void insert(T k, ValueType v)
//   {
//      union {
//         T x;
//         u8 key[sizeof(T)];
//      };
//      x = swap(k);
//      BTree::insert(key, sizeof(T), v);
//   }
//   bool remove(T k)
//   {
//      union {
//         T x;
//         u8 key[sizeof(T)];
//      };
//      x = swap(k);
//      return BTree::remove(key, sizeof(T));
//   }
//   bool lookup(T k, ValueType &result)
//   {
//      union {
//         T x;
//         u8 s[sizeof(T)];
//      };
//      x = swap(k);
//      return BTree::lookup(s, sizeof(T), result);
//   }
//};

using KeyType  = u32;

int main(int argc, char **argv)
{
   BTree tree;
   uint64_t totalSpace = 0;
   string payload;
   string input;


   if ( argc < 2 ) {
      return 0;
   }
   PerfEvent e;
   cout << "file time" << endl;
   ifstream in(argv[1]);

   vector<string> data;
   string line;
   while ( getline(in, line))
      data.push_back(line);
   uint64_t count = data.size();
   for ( auto &s : data )
      totalSpace += (s.size() * 2 + 1);

   union {
      u64 x;
      u8 bytes[sizeof(u64)];
   };
   u64 payload_length;
   {
      cout << "insert" << endl;
      e.setParam("op", "insert");
      PerfEventBlock b(e, count);
      for ( uint64_t i = 0; i < count; i++ ) {
         x = i;
         tree.insert((u8 *) data[i].data(), data[i].size(), sizeof(u64), bytes);
      }
   }
   {
      cout << "lookup" << endl;
      e.setParam("op", "lookup");
      PerfEventBlock b(e, count);
      for ( uint64_t i = 0; i < count; i++ ) {
         if ( !tree.lookup((u8 *) data[i].data(), data[i].size(), payload_length, bytes))
            throw;
         if ( x != i )
            throw;
      }
      printInfos(tree.root, totalSpace);
   }
   if ( getenv("DEL")) {
      cout << "remove" << endl;
      e.setParam("op", "remove");
      PerfEventBlock b(e, count);
      for ( uint64_t i = 0; i < count; i++ ) {
         tree.remove((u8 *) data[i].data(), data[i].size());
         if ( tree.lookup((u8 *) data[i].data(), data[i].size(), payload_length, bytes))
            throw;
      }
   }
   printInfos(tree.root, count);


   cout << "please enter: " << endl;
   cin >> input;
   while ( getenv("I")) {
      if ( input[0] == 'i' ) {
         cout << "insert mode: " << endl;
         cin >> input;
         cin >> payload;
         u64 key_length = input.length();
         u64 payload_length = payload.length();
         tree.insert((u8 *) input.data(), key_length, payload_length, (u8 *) payload.data());
         totalSpace += key_length + payload_length;
      } else if ( input[0] == 'l' ) {
         cin >> input;
         u64 payloadLength;
         auto payload_array = make_unique<u8[]>(100);
         if ( tree.lookup((u8 *) input.data(), input.length(), payloadLength, bytes)) {
            cout << "found: " << payloadLength << " - " << x << endl;
         } else {
            cout << "not found" << endl;
         }
      } else if ( input[0] == 'd' ) {
         cin >> input;
         tree.remove((u8 *) input.data(), input.length());
      } else if ( input[0] == 'p' ) {
         printInfos(tree.root, totalSpace);
      } else if ( input[0] == 'c' ) {
         cout << "goodbye" << endl;
         break;
      }
      cout << "please enter: " << endl;
      cin >> input;
   }
   return 0;
}

/*
if n underfull:
1. ensureFull(Node* n)
2. traverse from root
3. merge(Node* parent, Node* left, Node* right)
   3.1 make tmp node
   3.2 copy left, copy right
   3.3. copy tmp to left
   3.4. delete separator in parent

-ignore long fence keys
-long string: if larger than 1/4 of page ->
 store separately (as extra-long malloc string)
 need special case comparison between two extra-long strings

*/


