# LeanStore

[LeanStore](https://db.in.tum.de/~leis/papers/leanstore.pdf) is a high-performance OLTP storage engine optimized for many-core CPUs and NVMe SSDs. Our goal is to achieve performance comparable to in-memory systems when the data set fits into RAM, while being able to fully exploit the bandwidth of fast NVMe SSDs for large data sets. While LeanStore is currently a research prototype, we hope to make it usable in production in the future.

## Compiling
Install dependencies:

### Core

`sudo apt-get install autoconf automake libtool curl make cmake g++ unzip libtbb-dev libfmt-dev libgflags-dev libgtest-dev libgmock-dev liburing-dev libzstd-dev libcurl4-openssl-dev libbenchmark-dev`

### Third-party libraries

**Databases**: `sudo apt-get install libwiredtiger-dev libsqlite3-dev libmysqlcppconn-dev libpq-dev libfuse-dev`

## How to build

`mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j`
