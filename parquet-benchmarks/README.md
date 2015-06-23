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
  
##Running Parquet Benchmarks

First, build the ``parquet-benchmarks`` module

```
mvn --projects parquet-benchmarks -amd -DskipTests -Denforcer.skip=true clean package
```

Then, you can run all the benchmarks with the following command (use parquetVersion=vN to specify
the parquet file format version)

```
  ./parquet-benchmarks/run.sh -wi 5 -i 5 -f 3 -p parquetVersion=v1     # Run all benchmarks with PARQUET_1_0
  ./parquet-benchmarks/run.sh -wi 5 -i 5 -f 3 -p parquetVersion=v2     # Run all benchmarks with PARQUET_2_0
```

Also, you can run only write or read benchmarks with the following command

```
 ./parquet-benchmarks/write-benchmark.sh -wi 5 -i 5 -f 3 -p parquetVersion=v2
 ./parquet-benchmarks/read-benchmark.sh -wi 5 -i 5 -f 3 -p parquetVersion=v2
```

While creating a test Parquet file of N records, by default all benchmarks use a single record
that's repeated N times throughout the file. To have a larger number of random records in
this test file you can specify `randomData=true` -- with this flag 100K random records
are generated and used repeatedly while creating the test file.

```
  # Run all benchmarks with PARQUET_2_0 and using random data
  ./parquet-benchmarks/run.sh -wi 5 -i 5 -f 3 -p parquetVersion=v2 -p randomData=true
```

To understand what each command line argument means and for more arguments please see

```
java -jar parquet-benchmarks/target/parquet-benchmarks.jar -help
```