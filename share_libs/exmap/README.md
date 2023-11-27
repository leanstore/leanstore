# ExMap - Fully explicit memory-mapped file I/O

**Original repository**: https://github.com/tuhhosg/exmap

The exmap kernel module creates a memory area that isn't managed by the Linux kernel, In that area, memory allocation and freeing, as well as reads and writes, can only be done explicitly by the applications using exmap. One possible use case is the buffer manager of a database.

For further information see the `doc/` folder.

## How to build and run

```
./load.sh
```

Run `./test-exmap` in the `eval/` folder for a basic functionality test
