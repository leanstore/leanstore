# LeanStore

[LeanStore](https://db.in.tum.de/~leis/papers/leanstore.pdf) is a high-performance OLTP storage engine optimized for many-core CPUs and NVMe SSDs. Our goal is to achieve performance comparable to in-memory systems when the data set fits into RAM, while being able to fully exploit the bandwidth of fast NVMe SSDs for large data sets. While LeanStore is currently a research prototype, we hope to make it usable in production in the future.

## Requirements

### Hardware

- Multiple-core CPUs, best with ~ 100 cores
- [A battery-backed NVMe SSD](https://www.memorysolution.de/produkte-online-shop/ssd-hdd/datacenter-ssd)

### Software

- **Requirement**: LeanStore depends on `exmap`, which is currently fixed with Linux kernel 6.11
  - If anyone wants to test LeanStore with other kernel, please [send an email to the author](mailto:lamduy.nguyen@tum.de)

## Implemented Featuers

- [x] Autonomous commit and barrier transactions [SIGMOD'25]
- [x] Variable-sized objects, File system interface, and virtual-memory aliasing [ICDE'24]
- [x] Virtual-memory assisted buffer manager with explicit OS pagetable management [SIDMOG'23]
- [x] Optimstic Lock Coupling with Hybrid Page Guard to synchronize paged data structures [IEEE'19]
- [x] Variable-length key/values B-Tree with prefix compression and hints [BTW'23]
- [x] Distributed Logging with remote flush avoidance [SIGMOD'20]
- [ ] Recovery [SIGMOD'20]

## Dependencies

### Core

`sudo apt-get install cmake libtbb-dev libfmt-dev libgflags-dev libgtest-dev libgmock-dev libgcrypt-dev liburing-dev libzstd-dev libbenchmark-dev libssl-dev`

**exmap**: stored in `share_libs/exmap`
- Run `sudo ./load.sh`

### Third-party databases

**Databases**: `sudo apt-get install libwiredtiger-dev libsqlite3-dev libmysqlcppconn-dev libpq-dev libfuse-dev`

## Usage

### Compiling

`mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j`

### Testing

`cd build && make test`
