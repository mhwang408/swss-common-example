#!/bin/bash
set -e

if [ ! -f /usr/local/lib/libswsscommon.so ]; then
  mkdir -p /usr/local/{bin,lib,include,share}
  cd /home/ubuntu/ows-example/src/sonic-swss-common
  ./autogen.sh
  PYTHON3=/usr/bin/python3 ./configure \
    --prefix=/usr/local \
    --disable-python2 \
    --disable-yangmodules
  bear -- make -j"$(nproc)"
  make install
fi

exec "$@"
