#!/bin/bash
set -e

if [ ! -f /usr/local/lib/libswsscommon.so ]; then
  mkdir -p /usr/local/{bin,lib,include,share}
  mkdir -p /var/run/redis/sonic-db
  cd /home/ubuntu/swss-common-example/src/sonic-swss-common
  ./autogen.sh
  PYTHON3=/usr/bin/python3 ./configure \
    --prefix=/usr/local \
    --disable-python2 \
    --disable-yangmodules
  bear -- make -j"$(nproc)"
  make install
  ldconfig
  # Autotools sometimes skips the Python wrapper files; ensure they exist.
  pkgdir=/usr/local/lib/python3/dist-packages/swsscommon
  cp -n pyext/py3/__init__.py "$pkgdir/" 2>/dev/null || true
  cp -n pyext/py3/swsscommon.py "$pkgdir/" 2>/dev/null || true
fi

cd /home/ubuntu/swss-common-example
mkdir -p /var/run/redis/sonic-db
rm -f /var/run/redis/sonic-db/database_config.json
cp database_config.json /var/run/redis/sonic-db/database_config.json
chmod 644 /var/run/redis/sonic-db/database_config.json
exec "$@"
