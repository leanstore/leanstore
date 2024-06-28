#!/usr/bin/env bash
export DEBIAN_FRONTEND=noninteractive
export DEBIAN_PRIORITY=critical
apt-get -qy update
apt-get -qy \
  -o "Dpkg::Options::=--force-confdef" \
  -o "Dpkg::Options::=--force-confold" upgrade
apt-get -qy --no-install-recommends install apt-utils
apt-get -qy autoclean

# Install packages via the system package-manager (apt-get)
apt-get -qy install \
 cmake \
 build-essential \
 git \
 libaio-dev \
 libbz2-dev \
 liblmdb++-dev \
 liblmdb-dev \
 liblz4-dev \
 librocksdb-dev \
 libsnappy-dev \
 libtbb-dev \
 liburing-dev \
 libwiredtiger-dev \
 libzstd-dev \
 zlib1g-dev


