FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONPATH=/usr/local/lib/python3/dist-packages
ENV LD_LIBRARY_PATH=/usr/local/lib

RUN apt-get update && apt-get install -y --no-install-recommends \
    autoconf \
    automake \
    build-essential \
    ca-certificates \
    debhelper \
    dh-exec \
    libboost-dev \
    libboost-serialization-dev \
    libgmock-dev \
    libgtest-dev \
    libhiredis-dev \
    libnl-3-dev \
    libnl-genl-3-dev \
    libnl-nf-3-dev \
    libnl-route-3-dev \
    libtool \
    libzmq3-dev \
    make \
    nlohmann-json3-dev \
    pkg-config \
    python3 \
    python3-dev \
    swig \
    uuid-dev \
    && rm -rf /var/lib/apt/lists/*

COPY sonic-swss-common /tmp/sonic-swss-common

RUN cd /tmp/sonic-swss-common \
    && ./autogen.sh \
    && PYTHON3=/usr/bin/python3 ./configure \
        --prefix=/usr/local \
        --disable-python2 \
        --disable-yangmodules \
    && make -j"$(nproc)" install \
    && rm -rf /tmp/sonic-swss-common

RUN mkdir -p /usr/share \
    && ln -s /usr/local/share/swss /usr/share/swss

ENV PYTHONUNBUFFERED=1

WORKDIR /workspace

ENTRYPOINT ["python3", "examples/custom_tables/config_to_appl_bridge.py"]
CMD ["--key", "demo", "--watch"]
