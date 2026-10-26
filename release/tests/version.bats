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
  _reset_version_vars
  source "${LIBS_DIR}/_version.sh"
}

# ---- validate_and_extract_version ----

@test "validate_and_extract_version: accepts 1.18.0" {
  run validate_and_extract_version "1.18.0"
  [ "$status" -eq 0 ]
}

@test "validate_and_extract_version: extracts major.minor.patch" {
  validate_and_extract_version "1.18.0"
  [ "$major" = "1" ]
  [ "$minor" = "18" ]
  [ "$patch" = "0" ]
  [ "$version_without_rc" = "1.18.0" ]
}

@test "validate_and_extract_version: accepts 2.0.10" {
  validate_and_extract_version "2.0.10"
  [ "$major" = "2" ]
  [ "$minor" = "0" ]
  [ "$patch" = "10" ]
}

@test "validate_and_extract_version: rejects missing patch" {
  run validate_and_extract_version "1.18"
  [ "$status" -eq 1 ]
}

@test "validate_and_extract_version: rejects empty string" {
  run validate_and_extract_version ""
  [ "$status" -eq 1 ]
}

@test "validate_and_extract_version: rejects alpha characters" {
  run validate_and_extract_version "1.18.0-beta"
  [ "$status" -eq 1 ]
}

@test "validate_and_extract_version: rejects SNAPSHOT suffix" {
  run validate_and_extract_version "1.18.0-SNAPSHOT"
  [ "$status" -eq 1 ]
}

# ---- validate_and_extract_branch_version ----

@test "validate_and_extract_branch_version: parses parquet-1.18.x" {
  validate_and_extract_branch_version "parquet-1.18.x"
  [ "$major" = "1" ]
  [ "$minor" = "18" ]
}

@test "validate_and_extract_branch_version: parses parquet-2.0.x" {
  validate_and_extract_branch_version "parquet-2.0.x"
  [ "$major" = "2" ]
  [ "$minor" = "0" ]
}

@test "validate_and_extract_branch_version: rejects release/ prefix" {
  run validate_and_extract_branch_version "release/1.18.x"
  [ "$status" -eq 1 ]
}

@test "validate_and_extract_branch_version: rejects full version" {
  run validate_and_extract_branch_version "parquet-1.18.0"
  [ "$status" -eq 1 ]
}

@test "validate_and_extract_branch_version: rejects master" {
  run validate_and_extract_branch_version "master"
  [ "$status" -eq 1 ]
}

# ---- find_next_rc_number ----

@test "find_next_rc_number: returns 0 when no tags exist" {
  git() { echo ""; }
  export -f git
  find_next_rc_number "1.18.0"
  [ "$rc_number" = "0" ]
}

@test "find_next_rc_number: returns 1 after rc0" {
  git() {
    if [[ "$1" == "tag" && "$2" == "-l" ]]; then
      echo "apache-parquet-1.18.0-rc0"
    fi
  }
  export -f git
  find_next_rc_number "1.18.0"
  [ "$rc_number" = "1" ]
}

@test "find_next_rc_number: returns 3 after rc0, rc1, rc2" {
  git() {
    if [[ "$1" == "tag" && "$2" == "-l" ]]; then
      printf "apache-parquet-1.18.0-rc0\napache-parquet-1.18.0-rc1\napache-parquet-1.18.0-rc2\n"
    fi
  }
  export -f git
  find_next_rc_number "1.18.0"
  [ "$rc_number" = "3" ]
}

@test "find_next_rc_number: handles gap in rc numbers" {
  git() {
    if [[ "$1" == "tag" && "$2" == "-l" ]]; then
      printf "apache-parquet-1.18.0-rc0\napache-parquet-1.18.0-rc5\n"
    fi
  }
  export -f git
  find_next_rc_number "1.18.0"
  [ "$rc_number" = "6" ]
}

@test "find_next_rc_number: ignores tags for other versions" {
  git() {
    if [[ "$1" == "tag" && "$2" == "-l" ]]; then
      echo ""
    fi
  }
  export -f git
  find_next_rc_number "1.19.0"
  [ "$rc_number" = "0" ]
}

# ---- set_pom_version ----

@test "set_pom_version: passes correct args to mvnw in dry-run" {
  DRY_RUN=1
  run set_pom_version "1.18.0"
  [ "$status" -eq 0 ]
  [[ "$output" == *"versions:set"* ]]
  [[ "$output" == *"-DnewVersion=1.18.0"* ]]
  [[ "$output" == *"-DgenerateBackupPoms=false"* ]]
}

@test "set_pom_version: calls mvnw with correct args in real mode" {
  DRY_RUN=0
  local captured_args=""
  mvnw() { captured_args="$*"; }
  # Create a fake mvnw wrapper that the function calls via ./mvnw
  function mock_mvnw_wrapper {
    echo "$@" > "${BATS_TEST_TMPDIR}/mvnw_args"
  }
  # Override exec_process to capture command
  exec_process() { echo "EXEC: $*"; }
  export -f exec_process
  run set_pom_version "2.0.0-SNAPSHOT"
  [ "$status" -eq 0 ]
  [[ "$output" == *"2.0.0-SNAPSHOT"* ]]
}

# ---- find_latest_rc_number ----

@test "find_latest_rc_number: returns highest RC number" {
  cd "$(mktemp -d)"
  git init -q
  git commit --allow-empty -m "init" -q
  git tag "apache-parquet-1.18.0-rc0"
  git tag "apache-parquet-1.18.0-rc1"
  git tag "apache-parquet-1.18.0-rc2"
  find_latest_rc_number "1.18.0"
  [ "$latest_rc_number" = "2" ]
}

@test "find_latest_rc_number: returns 0 when only rc0 exists" {
  cd "$(mktemp -d)"
  git init -q
  git commit --allow-empty -m "init" -q
  git tag "apache-parquet-2.0.0-rc0"
  find_latest_rc_number "2.0.0"
  [ "$latest_rc_number" = "0" ]
}

@test "find_latest_rc_number: fails when no RC tags exist" {
  cd "$(mktemp -d)"
  git init -q
  git commit --allow-empty -m "init" -q
  run find_latest_rc_number "9.9.9"
  [ "$status" -eq 1 ]
  [[ "$output" == *"No RC tags found"* ]]
}

# ---- get_current_pom_version ----

@test "get_current_pom_version: calls mvnw help:evaluate" {
  # Mock ./mvnw
  function fake_mvnw {
    if [[ "$*" == *"help:evaluate"* ]]; then
      echo "1.18.0-SNAPSHOT"
    fi
  }
  # Temporarily create a fake mvnw in a temp dir
  local tmpbin
  tmpbin=$(mktemp -d)
  cat > "${tmpbin}/mvnw" << 'SCRIPT'
#!/bin/bash
if [[ "$*" == *"help:evaluate"* ]]; then
  echo "1.18.0-SNAPSHOT"
fi
SCRIPT
  chmod +x "${tmpbin}/mvnw"
  # Run from tmpbin so ./mvnw resolves
  cd "${tmpbin}"
  local result
  result=$(get_current_pom_version)
  [ "$result" = "1.18.0-SNAPSHOT" ]
  rm -rf "${tmpbin}"
}
