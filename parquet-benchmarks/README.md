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
  
# Running Parquet Benchmarks

The Parquet benchmarks in this module are run using the 
[OpenJDK Java Microbenchmarking Harness](http://openjdk.java.net/projects/code-tools/jmh/).

First, building the `parquet-benchmarks` module creates an uber-jar including the Parquet
classes and all dependencies, and a main class to launch the JMH tool.

```
mvn --projects parquet-benchmarks -amd -DskipTests -Denforcer.skip=true clean package
```

JMH doesn't have the notion of "benchmark suites", but there are certain benchmarks that 
make sense to group together or to run in isolation during development.  The 
`./parquet-benchmarks/run.sh` script can be used to launch all or some benchmarks:

```
# More information about the run script and the available arguments.
./parquet-benchmarks/run.sh

# More information on the JMH options available.
./parquet-benchmarks/run.sh all -help

# Run every benchmark once (~20 minutes).
./parquet-benchmarks/run.sh all -wi 0 -i 1 -f 1

# A more rigourous run of all benchmarks, saving a report for comparison.
./parquet-benchmarks/run.sh all -wi 5 -i 5 -f 3 -rff /tmp/benchmark1.json

# Run a benchmark "suite" built into the script, with JMH defaults (about 30 minutes)
./parquet-benchmarks/run.sh checksum

# Running one specific benchmark using a regex.
./parquet-benchmarks/run.sh all org.apache.parquet.benchmarks.NestedNullWritingBenchmarks

# Manually clean up any state left behind from a previous run.
./parquet-benchmarks/run.sh clean
```

