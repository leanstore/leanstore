# LeanStore and BookKeeper integration

Replacing the Write-Ahead-Log (WAL) with BookKeeper.

## Compiling

In addition to getting the dependencies for LeanStore, we need to install Java 17 to support our wrapper for BookKeeper's `LedgerHandle`.

Make sure that `JAVA_HOME` refers to the correct directory for Java 17 and `CXX` refers to a compiler that supports the C++20 standard. On the c09 cluster, we can use the following commands before compiling.

```bash
export CXX=clang++-14
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
```

Here, clang is used because there are some linker issues with gcc on the cluster. Then we can start compiling the Java wrapper and LeanStore.

```bash
cd bookkeeper-helper && mvn package && cd .. && mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make tpcc
```

We only interest in the TPC-C benchmark for now.

## Runtime flags

We introduced the following flags to support BookKeeper.
+ `--bookkeeper_jar_directories`: List of directories containing all neccessary classpaths separated by ':'
+ `--bookkeeper_metadata_uri`: URI to BookKeeper's metadata service
+ `--bookkeeper_ensemble`: BookKeeper ledger ensemble size
+ `--bookkeeper_quorum`: BookKeeper ledger quorum size

## Benchmark examples

From the project root directory, we can run the minimal TPC-C benchmark using the following commands.

```
touch leanstore && ./build/frontend/tpcc --bookkeeper_jar_directories=bookkeeper-helper/target:bookkeeper-helper/target/maven-dependencies --wal_pwrite --wal_variant 3
```
