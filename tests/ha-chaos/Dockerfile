# syntax=docker/dockerfile:1.7
###############################################################################
# Mooncake HA Chaos Test Image — Lightweight, No CUDA
#
# Builds: mooncake_master, mooncake_client (real_client)
# Uses TCP transport only, no RDMA/CUDA
###############################################################################

FROM ubuntu:22.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1

ARG CMAKE_BUILD_TYPE=Release

# Install base build utilities and Python
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        build-essential \
        cmake \
        pkg-config \
        wget \
        patchelf \
        python3 \
        python3-dev \
        python3-pip \
        python-is-python3 \
        # Core libraries
        libibverbs-dev \
        libjsoncpp-dev \
        libcurl4-openssl-dev \
        libhiredis-dev \
        libgoogle-glog-dev \
        libgtest-dev \
        libunwind-dev \
        libnuma-dev \
        libboost-all-dev \
        libssl-dev \
        libyaml-cpp-dev \
        liburing-dev \
        libjemalloc-dev \
        libmsgpack-dev \
        libzstd-dev \
        libasio-dev \
        libxxhash-dev \
        libpython3-dev \
        libprotobuf-dev \
        libgrpc-dev \
        libgrpc++-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY . /workspace

# Install dependencies (yalantinglibs, Go, etc.)
RUN bash dependencies.sh -y

# Configure & build — NO CUDA, NO EP, TCP only, with etcd HA
RUN mkdir -p build && \
    cd build && \
    cmake .. \
        -DBUILD_UNIT_TESTS=OFF \
        -DUSE_HTTP=ON \
        -DUSE_ETCD=ON \
        -DUSE_CUDA=OFF \
        -DUSE_TCP=ON \
        -DWITH_EP=OFF \
        -DWITH_STORE=ON \
        -DSTORE_USE_ETCD=ON \
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} && \
    cmake --build . -j"$(nproc)"

# Build the Python wheel
RUN OUTPUT_DIR=dist BUILD_WITH_EP=0 ./scripts/build_wheel.sh

###############################################################################
# Runtime stage — minimal
###############################################################################
FROM ubuntu:22.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    MC_FORCE_TCP=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        # RDMA stubs (needed at link time even in TCP-only mode)
        ibverbs-providers \
        libibverbs1 \
        librdmacm1 \
        # Other runtime deps
        libnuma1 \
        liburing2 \
        libyaml-0-2 \
        libcurl4 \
        curl \
        etcd-client && \
    rm -rf /var/lib/apt/lists/*

# Copy binaries
COPY --from=builder /workspace/build/mooncake-store/src/mooncake_master /usr/local/bin/
COPY --from=builder /workspace/build/mooncake-store/src/mooncake_client /usr/local/bin/

# Copy and install Python wheel
COPY --from=builder /workspace/mooncake-wheel/dist /tmp/mooncake-wheel
RUN python3 -m pip install --no-cache-dir /tmp/mooncake-wheel/*.whl 2>/dev/null || true && \
    rm -rf /tmp/mooncake-wheel /root/.cache/pip

# Install Python test dependencies
RUN python3 -m pip install --no-cache-dir \
    requests \
    pytest \
    pytest-timeout \
    docker \
    tenacity \
    pyyaml

# Copy test files
COPY tests/ha-chaos/conf/ /etc/mooncake/
COPY tests/ha-chaos/tests/ /opt/chaos-tests/

WORKDIR /opt/chaos-tests

CMD ["/bin/bash"]
