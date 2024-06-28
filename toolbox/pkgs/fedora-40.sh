#!/usr/bin/env bash

# Install packages via the system package-manager (dnf)
dnf install -y \
 bzip2-devel \
 bzip2-static \
 cmake \
 clang-tools-extra \
 dh-autoreconf \
 g++ \
 git \
 libaio-devel \
 libatomic \
 libstdc++ \
 libstdc++-static \
 liburing-devel \
 libzstd-devel \
 lmdb-devel \
 lz4-devel \
 python \
 python3 \
 python3-devel \
 rocksdb-devel \
 snappy-devel \
 tbb-devel \
 zlib-devel

# Via source (wiredtiger)
git clone https://github.com/wiredtiger/wiredtiger.git
cd wiredtiger

# Build version 3.2.1, the project is way ahead of this version, yet, this
# is the version which is available via Ubuntu packages and thus, the version
# tested against, thus building it rather than the latest and greatest
#
# NOTE: using 'make -j' breaks the build...
git checkout 3.2.1
./autogen.sh
./configure --prefix=/usr
make -j
make install 
