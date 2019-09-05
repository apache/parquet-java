#!/usr/bin/env bash
#
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
#

SCRIPT_PATH=$( cd "$(dirname "$0")" ; pwd -P )

BENCHMARK=$1; shift
JMH_OPTIONS="$@"

if [ -z "$BENCHMARK" ]; then

  # Print usage if run without arguments.
  cat << EOF
Runs Parquet JMH-based benchmarks.

Usage:
  run.sh <BENCHMARK> [JMH_OPTIONS]

Information on the JMH_OPTIONS can be found by running: run.sh all -help

<BENCHMARK> | Description
----------- | ----------
all         | Runs all benchmarks in the module (listed here and others).
build       | (No benchmark run, shortcut to rebuild the JMH uber jar).
clean       | (No benchmark run, shortcut to clean up any temporary files).
read        | Reading files with different compression, page and block sizes.
write       | Writing files.
checksum    | Reading and writing with and without CRC checksums.
filter      | Filtering column indexes

Examples:

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

EOF

elif [ "$BENCHMARK" == "build" ]; then

  # Shortcut utility to rebuild the benchmark module only.
  ( cd $SCRIPT_PATH && mvn -amd -DskipTests -Denforcer.skip=true clean package  )

elif [ "$BENCHMARK" == "clean" ]; then

  # Shortcut utility to clean any state left behind from any previous run.
  java -cp ${SCRIPT_PATH}/target/parquet-benchmarks.jar org.apache.parquet.benchmarks.DataGenerator cleanup  java -jar ${SCRIPT_PATH}/target/parquet-benchmarks.jar org.apache.parquet.benchmarks.PageChecksumReadBenchmarks -bm ss "$@"

else

  # Actually run a benchmark in the JMH harness.

  # Pick a regex if specified.
  BENCHMARK_REGEX=""
  case "$BENCHMARK" in
  "read")
    BENCHMARK_REGEX="org.apache.parquet.benchmarks.ReadBenchmarks"
    ;;
  "write")
    BENCHMARK_REGEX="org.apache.parquet.benchmarks.WriteBenchmarks"
    ;;
  "checksum")
    BENCHMARK_REGEX="org.apache.parquet.benchmarks.PageChecksum.*"
    ;;
  "filter")
    BENCHMARK_REGEX="org.apache.parquet.benchmarks.FilteringBenchmarks"
    ;;
  esac

  echo JMH command: java -jar ${SCRIPT_PATH}/target/parquet-benchmarks.jar $BENCHMARK_REGEX $JMH_OPTIONS
  java -jar ${SCRIPT_PATH}/target/parquet-benchmarks.jar $BENCHMARK_REGEX $JMH_OPTIONS
fi
