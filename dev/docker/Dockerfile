# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

FROM maven:3.9.8-eclipse-temurin-17

ARG THRIFT_VERSION=0.24.0
ARG THRIFT_SHA256=e0fa5839a4c5c1d631b0931cf2c554ebbfa4e2fee3a9fb3ffd4f82ce4396c6e4

USER root

RUN set -eux; \
    apt-get update; \
    apt-get install --yes --no-install-recommends \
      autoconf \
      automake \
      bison \
      ca-certificates \
      flex \
      g++ \
      libboost-dev \
      libevent-dev \
      libssl-dev \
      libtool \
      make \
      pkg-config \
      wget; \
    wget --quiet --output-document=/tmp/thrift.tar.gz \
      "https://archive.apache.org/dist/thrift/${THRIFT_VERSION}/thrift-${THRIFT_VERSION}.tar.gz"; \
    echo "${THRIFT_SHA256}  /tmp/thrift.tar.gz" | sha256sum --check --status; \
    mkdir /tmp/thrift-src; \
    tar --extract --gzip --file=/tmp/thrift.tar.gz --directory=/tmp/thrift-src --strip-components=1; \
    cd /tmp/thrift-src; \
    chmod +x ./configure; \
    ./configure --disable-libs; \
    make -j"$(nproc)"; \
    make install; \
    thrift -version | grep --fixed-strings "Thrift version ${THRIFT_VERSION}"; \
    rm -rf /tmp/thrift.tar.gz /tmp/thrift-src /var/lib/apt/lists/*

RUN useradd --create-home --user-group --shell /bin/bash parquet \
    && install --directory --owner=parquet --group=parquet /workspace /home/parquet/.m2

ENV MAVEN_CONFIG=/home/parquet/.m2
# TestLargeColumnChunk intentionally writes a 4 GiB row group.
ENV JAVA_TOOL_OPTIONS=-Xmx6g

USER parquet
WORKDIR /workspace

CMD ["bash"]
