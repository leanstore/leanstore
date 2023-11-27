## Repository file structure
Important files:

- driver.c: main implementation
- memory.c: C-c C-v from linux kernel memory.c, adapted
- ksyms.c/ksyms.h: get non-exported functions
- exmap.h: includes for the kernel module
- linux/exmap.h: includes for applications using exmap

Test/Benchmark files:

- test-exmap.cc: test the basic functionality of exmap (create/alloc/free)
- test-bench-alloc.cc: basic allocation/free benchmark (seen in the paper in Figure 8: exmap (1IF))
- test-bench-steal.cc: for alloc with stealing (Figure 8: exmap (2IF) `ifdef SAME_THREAD`, exmap (pool) otherwise)
- bench\_common.h: functions/classes/... used by multiple benchmark programs
