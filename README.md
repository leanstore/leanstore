# Autonomous Commit - submission for SIGMOD 25

Integrated into LeanStore, a research storage engine optimized for high-performance NVMe SSDs.

## Compiling
Install dependencies:

### Core

`sudo apt-get install cmake libtbb-dev libfmt-dev libgflags-dev libgtest-dev libgmock-dev liburing-dev libzstd-dev libbenchmark-dev`

**exmap**: stored in `share_libs/exmap`
- Run `sudo ./load.sh`

### Third-party libraries

**Databases**: `sudo apt-get install libwiredtiger-dev libsqlite3-dev libmysqlcppconn-dev libpq-dev libfuse-dev`

## How to build

`mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j`
