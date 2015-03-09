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
in the inspection of [Parquet files](https://github.com/Parquet).

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

#Run from hadoop

See Commands Usage for command to use

```sh
hadoop jar ./parquet-tools-<VERSION>.jar <command> my_parquet_file.lzo.parquet
```

#Run locally

See Commands Usage for command to use

```
java jar ./parquet-tools-<VERSION>.jar <command> my_parquet_file.lzo.parquet
```

## Commands Usage

To run it on hadoop, you should use "hadoop jar" instead of "java jar"

```sh
usage: java jar ./parquet-tools-<VERSION>.jar cat [option...] <input>
where option is one of:
       --debug     Disable color output even if supported
    -h,--help      Show this help string
       --no-color  Disable color output even if supported
where <input> is the parquet file to print to stdout

usage: java jar ./parquet-tools-<VERSION>.jar head [option...] <input>
where option is one of:
       --debug          Disable color output even if supported
    -h,--help           Show this help string
    -n,--records <arg>  The number of records to show (default: 5)
       --no-color       Disable color output even if supported
where <input> is the parquet file to print to stdout

usage: java jar ./parquet-tools-<VERSION>.jar schema [option...] <input>
where option is one of:
    -d,--detailed <arg>  Show detailed information about the schema.
       --debug           Disable color output even if supported
    -h,--help            Show this help string
       --no-color        Disable color output even if supported
where <input> is the parquet file containing the schema to show

usage: java jar ./parquet-tools-<VERSION>.jar meta [option...] <input>
where option is one of:
       --debug     Disable color output even if supported
    -h,--help      Show this help string
       --no-color  Disable color output even if supported
where <input> is the parquet file to print to stdout

usage: java jar dump [option...] <input>
where option is one of:
    -c,--column <arg>  Dump only the given column, can be specified more than
                       once
    -d,--disable-data  Do not dump column data
       --debug         Disable color output even if supported
    -h,--help          Show this help string
    -m,--disable-meta  Do not dump row group and page metadata
       --no-color      Disable color output even if supported
where <input> is the parquet file to print to stdout
```

