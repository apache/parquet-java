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

Parquet Tools
======

Parquet-Tools contains java based command line tools that aid
in the inspection of [Parquet files](https://parquet.apache.org).

Currently these tools are available for UN*X systems.

## Build

If you want to use parquet-tools in local mode, you should use the local profile so the 
hadoop client dependency is included.

```sh
cd parquet-tools && mvn clean package -Plocal 
```

To use it in hadoop mode, the default profile will exclude the hadoop client dependency

```sh
cd parquet-tools && mvn clean package 
```

The resulting jar is target/parquet-tools-<Version>.jar, you can copy it to the place where you
want to use it

### Run from hadoop

See Commands Usage for command to use

```sh
hadoop jar ./parquet-tools-<VERSION>.jar <command> my_parquet_file.lzo.parquet
```

### Run locally

See Commands Usage for command to use

```
java -jar ./parquet-tools-<VERSION>.jar <command> my_parquet_file.lzo.parquet
```

## Commands Usage

To see usage instructions for all commands: 

```
java -jar ./parquet-tools-<VERSION>.jar --help
```

**Note:** To run it on hadoop, you should use `hadoop jar` instead of `java -jar`

## Meta Legend

### Row Group Totals

Acronym | Definition
--------|-----------
RC | Row Count
TS | Total Byte Size

### Row Group Column Details

Acronym | Definition
--------|-----------
DO | Dictionary Page Offset
FPO | First Data Page Offset
SZ:{x}/{y}/{z} | Size in bytes. x = Compressed total, y = uncompressed total, z = y:x ratio
VC | Value Count
RLE | Run-Length Encoding
