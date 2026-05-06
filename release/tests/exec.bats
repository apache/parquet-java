#!/usr/bin/env bats
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

setup() {
  load test_helper/common
  source "${LIBS_DIR}/_exec.sh"
}

# ---- exec_process ----

@test "exec_process: dry-run prints but does not execute" {
  DRY_RUN=1
  run exec_process echo "should not appear as direct output"
  [ "$status" -eq 0 ]
  [[ "$output" == *"Dry-run, WOULD execute"* ]]
  [[ "$output" == *"echo"* ]]
}

@test "exec_process: real run executes command" {
  DRY_RUN=0
  run exec_process echo "hello from exec"
  [ "$status" -eq 0 ]
  [[ "$output" == *"hello from exec"* ]]
}

@test "exec_process: real run preserves exit code" {
  DRY_RUN=0
  run exec_process false
  [ "$status" -ne 0 ]
}

# ---- exec_process_with_retries ----

@test "exec_process_with_retries: succeeds on first attempt" {
  DRY_RUN=0
  run exec_process_with_retries 3 0 "" echo "ok"
  [ "$status" -eq 0 ]
  [[ "$output" == *"ok"* ]]
}

@test "exec_process_with_retries: fails after max attempts" {
  DRY_RUN=0
  run exec_process_with_retries 2 0 "" false
  [ "$status" -ne 0 ]
  [[ "$output" == *"failed after 2 attempts"* ]]
}

@test "exec_process_with_retries: requires at least 4 args" {
  DRY_RUN=0
  run exec_process_with_retries 3 0
  [ "$status" -ne 0 ]
}

# ---- calculate_sha512 ----

@test "calculate_sha512: creates checksum file in real mode" {
  DRY_RUN=0
  local tmpfile
  tmpfile=$(mktemp)
  echo "test content" > "$tmpfile"

  calculate_sha512 "$tmpfile"

  [ -f "${tmpfile}.sha512" ]
  # The checksum file should contain the filename
  local basename
  basename=$(basename "$tmpfile")
  [[ "$(cat "${tmpfile}.sha512")" == *"${basename}"* ]]

  rm -f "$tmpfile" "${tmpfile}.sha512"
}

@test "calculate_sha512: dry-run does not create file" {
  DRY_RUN=1
  local tmpfile
  tmpfile=$(mktemp)
  echo "test content" > "$tmpfile"

  run calculate_sha512 "$tmpfile"
  [ "$status" -eq 0 ]
  [ ! -f "${tmpfile}.sha512" ]

  rm -f "$tmpfile"
}
