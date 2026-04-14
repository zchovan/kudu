FROM apache/kudu:kudu-python-1.18.1-ubuntu AS kudu-base

FROM ubuntu:20.04 AS build
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      ca-certificates curl git g++ gcc make cmake pkg-config zlib1g-dev \
      libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

ENV KUDU_HOME=/usr/local
ENV LD_LIBRARY_PATH=/usr/local/lib

COPY --from=kudu-base /usr/local/include /usr/local/include
COPY --from=kudu-base /usr/local/lib /usr/local/lib

WORKDIR /app
COPY . /app

RUN rm -rf build \
    && mkdir -p build \
    && cd build \
    && cmake .. \
    && make -j"$(nproc)"

FROM apache/kudu:kudu-python-1.18.1-ubuntu

ENV KUDU_HOME=/usr/local
ENV LD_LIBRARY_PATH=/usr/local/lib

WORKDIR /app
COPY --from=build /app/build/kudu_insert /app/build/kudu_insert

ENTRYPOINT ["/app/build/kudu_insert"]
