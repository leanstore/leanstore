#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <iostream>
// -------------------------------------------------------------------------------------
int main(int argc, char **argv)
{
   gflags::SetUsageMessage("Leanstore Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   return 0;
}