FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONPATH=/usr/local/lib/python3/dist-packages
ENV LD_LIBRARY_PATH=/usr/local/lib

RUN apt-get update && apt-get install -y --no-install-recommends \
    autoconf \
    automake \
    bear \
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

RUN mkdir -p /usr/share \
    && ln -s /usr/local/share/swss /usr/share/swss

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV PYTHONUNBUFFERED=1

WORKDIR /home/ubuntu/swss-common-example

ENTRYPOINT ["/entrypoint.sh"]
# CMD ["python3", "src/custom_tables/config_to_appl_bridge.py", "--key", "demo", "--watch"]
