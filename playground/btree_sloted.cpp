#include "BTreeSlotted.hpp"
// -------------------------------------------------------------------------------------
template<class T>
struct BTreeAdapter : public BTree {
   void insert(T k, ValueType v)
   {
      union {
         T x;
         u8 key[sizeof(T)];
      };
      x = swap(k);
      BTree::insert(key, sizeof(T), v);
   }
   bool remove(T k)
   {
      union {
         T x;
         u8 key[sizeof(T)];
      };
      x = swap(k);
      return BTree::remove(key, sizeof(T));
   }
   bool lookup(T k, ValueType &result)
   {
      union {
         T x;
         u8 s[sizeof(T)];
      };
      x = swap(k);
      return BTree::lookup(s, sizeof(T), result);
   }
};

using KeyType  = u32;

int main(int argc, char **argv)
{
   PerfEvent e;

   if ( getenv("I")) {
      BTree tree;
      uint64_t totalSpace = 0;
      u64 value;
      string input;
      cout << "please enter: " << endl;
      cin >> input;
      while ( true ) {
         if ( input[0] == 'i' ) {
            cout << "insert mode: " << endl;
            cin >> input;
            cin >> value;
            tree.insert((u8 *) input.data(), input.length(), reinterpret_cast<ValueType>(value));
            totalSpace+= sizeof(u64) + input.length();
         } else if ( input[0] == 'l' ) {
            cin >> input;
            if ( tree.lookup((u8 *) input.data(), input.length(), reinterpret_cast<ValueType &>(value))) {
               cout << "lookup: " << value << endl;
            } else {
               cout << "not found" << endl;
            }
         } else if ( input[0] == 'p' ) {
            printInfos(tree.root, totalSpace);
         } else if ( input[0] == 'c' ) {
            cout << "goodbye" << endl;
            return 0;
         }
         cout << "please enter: " << endl;
         cin >> input;
      }
   }
   if ( getenv("N")) {
      uint64_t n = atof(getenv("N"));
      vector<KeyType> v(n);
      for ( uint64_t i = 0; i < n; i++ )
         v[i] = i;
      random_shuffle(v.begin(), v.end());

      BTreeAdapter<KeyType> tree;
      {
         e.setParam("op", "insert");
         PerfEventBlock b(e, n);
         for ( uint64_t i = 0; i < n; i++ ) {
            if ( true || i % 4 != 0 )
               tree.insert(v[i], reinterpret_cast<ValueType>(i));
         }
      }
      {
         e.setParam("op", "lookup");
         PerfEventBlock b(e, n);
         for ( uint64_t i = 0; i < n; i++ ) {
            ValueType result;
            static_cast<void>(result);
            if ( true || i % 4 != 0 ) {
               if ( !(tree.lookup(v[i], result) && (reinterpret_cast<uint64_t>(result) == i)))
                  throw;
            } else {
               if ( tree.lookup(v[i], result))
                  throw;
            }
         }
      }
      printInfos(tree.root, n * (sizeof(KeyType) + sizeof(ValueType)));
      //exit(1);

      if ( getenv("DEL")) {
         e.setParam("op", "remove");
         PerfEventBlock b(e, n);
         for ( uint64_t i = 0; i < n; i++ ) {
            tree.remove(v[i]);
            /*uint64_t step = n/10;
            ValueType result;
            assert(!tree.lookup(v[i], result));
            for (uint64_t j=0; j<i; j+=step)
               if (tree.lookup(v[j], result))
                  throw;
            for (uint64_t j=i+1; j<n; j+=step) {
               if (!tree.lookup(v[j], result))
                  throw;
               if (reinterpret_cast<u64>(result) != j)
                  throw;
                  }*/
         }
      }
      printInfos(tree.root, n * (sizeof(KeyType) + sizeof(ValueType)));
      cout << endl;

      //return 0;
   }

   if ( argc < 2 ) {
      return 0;
   }
   cout << "file time" << endl;
   ifstream in(argv[1]);

   vector<string> data;
   string line;
   while ( getline(in, line))
      data.push_back(line);
   uint64_t count = data.size();
   uint64_t totalSpace = 0;
   for ( auto &s : data )
      totalSpace += (s.size() + sizeof(ValueType));

   BTree t;
   {
      e.setParam("op", "insert");
      PerfEventBlock b(e, count);
      for ( uint64_t i = 0; i < count; i++ ) {
         if ( true || i % 4 != 0 ) {
            t.insert((u8 *) data[i].data(), data[i].size(), reinterpret_cast<ValueType>(i));
         }
      }
   }
   {
      e.setParam("op", "lookup");
      PerfEventBlock b(e, count);
      for ( uint64_t i = 0; i < count; i++ ) {
         ValueType result;
         if ( true || i % 4 != 0 ) {
            if ( !t.lookup((u8 *) data[i].data(), data[i].size(), result))
               throw;
            if ((reinterpret_cast<uint64_t>(result) != i))
               throw;
         } else {
            if ( t.lookup((u8 *) data[i].data(), data[i].size(), result))
               throw;
         }
      }
   }
   printInfos(t.root, totalSpace);
   if ( getenv("DEL")) {
      e.setParam("op", "remove");
      PerfEventBlock b(e, count);
      for ( uint64_t i = 0; i < count; i++ ) {
         t.remove((u8 *) data[i].data(), data[i].size());
         ValueType result;
         if ( t.lookup((u8 *) data[i].data(), data[i].size(), result))
            throw;
      }
   }
   printInfos(t.root, totalSpace);

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


