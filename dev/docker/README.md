<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# Parquet developer container

**Status:** Experimental developer tooling. This Dockerfile is not officially supported.

This image provides the Java 17, Maven 3.9.8, and Thrift 0.24.0 toolchain used to build Parquet-Java. It deliberately does not copy project sources into the image: mount the checkout you are actively editing at `/workspace`.

## Build the image

From the repository root, run:

```bash
docker build --tag parquet-java-dev --file dev/docker/Dockerfile dev/docker
```

Verify the container's Thrift compiler:

```bash
docker run --rm parquet-java-dev thrift -version
```

## Work in the container

Start an interactive shell with the current checkout and a persistent Maven dependency cache:

```bash
docker run --rm --init --interactive --tty \
  --mount "type=bind,src=$PWD,dst=/workspace" \
  --mount type=volume,src=parquet-java-m2,dst=/home/parquet/.m2 \
  parquet-java-dev
```

Build and test the local checkout without opening a shell:

```bash
docker run --rm --init \
  --mount "type=bind,src=$PWD,dst=/workspace" \
  --mount type=volume,src=parquet-java-m2,dst=/home/parquet/.m2 \
  parquet-java-dev \
  ./mvnw --batch-mode test
```

The full suite includes a test that writes a 4 GiB row group. Configure Docker with at least 8 GiB of memory; the image gives JVM processes a 6 GiB maximum heap by default. Override that setting when needed with `--env JAVA_TOOL_OPTIONS=-Xmx<heap-size>`.

## Reuse the host Maven cache

The commands above use the persistent `parquet-java-m2` Docker volume. To reuse the local Maven repository instead, replace its volume mount with:

```bash
--mount "type=bind,src=$HOME/.m2,dst=/home/parquet/.m2"
```
