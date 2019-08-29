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

echo "Starting WRITE benchmarks"
java -jar ${SCRIPT_PATH}/target/parquet-benchmarks.jar org.apache.parquet.benchmarks.WriteBenchmarks "$@"
echo "Generating test data"
java -cp ${SCRIPT_PATH}/target/parquet-benchmarks.jar org.apache.parquet.benchmarks.DataGenerator generate
echo "Data generated, starting READ benchmarks"
java -jar ${SCRIPT_PATH}/target/parquet-benchmarks.jar org.apache.parquet.benchmarks.ReadBenchmarks "$@"
echo "Cleaning up generated data"
java -cp ${SCRIPT_PATH}/target/parquet-benchmarks.jar org.apache.parquet.benchmarks.DataGenerator cleanup
