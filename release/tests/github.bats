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
  source "${LIBS_DIR}/_github.sh"
}

# ---- check_github_checks_passed ----

@test "check_github_checks_passed: fails when GITHUB_TOKEN not set" {
  unset GITHUB_TOKEN
  DRY_RUN=0
  run check_github_checks_passed "abc123"
  [ "$status" -eq 1 ]
  [[ "$output" == *"GITHUB_TOKEN is required"* ]]
}

@test "check_github_checks_passed: skips in dry-run even without GITHUB_TOKEN" {
  unset GITHUB_TOKEN
  DRY_RUN=1
  run check_github_checks_passed "abc123"
  [ "$status" -eq 0 ]
  [[ "$output" == *"DRY_RUN"* ]]
}

@test "check_github_checks_passed: skips in dry-run mode" {
  export GITHUB_TOKEN="fake-token"
  DRY_RUN=1
  run check_github_checks_passed "abc123"
  [ "$status" -eq 0 ]
  [[ "$output" == *"DRY_RUN"* ]]
}

@test "check_github_checks_passed: succeeds when all checks completed and passed" {
  export GITHUB_TOKEN="fake-token"
  DRY_RUN=0

  gh() {
    cat <<'JSON'
{"workflow_runs": [
  {"name": "CI Hadoop 3",    "path": ".github/workflows/ci-hadoop3.yml",     "status": "completed", "conclusion": "success"},
  {"name": "Vector Plugins", "path": ".github/workflows/vector-plugins.yml", "status": "completed", "conclusion": "success"}
]}
JSON
  }
  export -f gh

  run check_github_checks_passed "abc123"
  [ "$status" -eq 0 ]
  [[ "$output" == *"All GitHub checks passed"* ]]
}

@test "check_github_checks_passed: fails when checks are still running" {
  export GITHUB_TOKEN="fake-token"
  DRY_RUN=0

  gh() {
    cat <<'JSON'
{"workflow_runs": [
  {"name": "CI Hadoop 3", "path": ".github/workflows/ci-hadoop3.yml", "status": "in_progress", "conclusion": null}
]}
JSON
  }
  export -f gh

  run check_github_checks_passed "abc123"
  [ "$status" -eq 1 ]
  [[ "$output" == *"still-running"* ]]
  [[ "$output" == *"CI Hadoop 3"* ]]
}

@test "check_github_checks_passed: fails when checks have failed conclusions" {
  export GITHUB_TOKEN="fake-token"
  DRY_RUN=0

  gh() {
    cat <<'JSON'
{"workflow_runs": [
  {"name": "CI Hadoop 3", "path": ".github/workflows/ci-hadoop3.yml", "status": "completed", "conclusion": "failure"}
]}
JSON
  }
  export -f gh

  run check_github_checks_passed "abc123"
  [ "$status" -eq 1 ]
  [[ "$output" == *"failed GitHub checks"* ]]
  [[ "$output" == *"CI Hadoop 3"* ]]
}

@test "check_github_checks_passed: ignores in-progress release-*.yml self-reference" {
  export GITHUB_TOKEN="fake-token"
  DRY_RUN=0

  gh() {
    cat <<'JSON'
{"workflow_runs": [
  {"name": "Release - Prepare RC", "path": ".github/workflows/release-prepare-rc.yml", "status": "in_progress", "conclusion": null},
  {"name": "CI Hadoop 3",          "path": ".github/workflows/ci-hadoop3.yml",         "status": "completed",   "conclusion": "success"}
]}
JSON
  }
  export -f gh

  run check_github_checks_passed "abc123"
  [ "$status" -eq 0 ]
  [[ "$output" == *"All GitHub checks passed"* ]]
}

@test "check_github_checks_passed: ignores historical failed release-*.yml on same commit" {
  export GITHUB_TOKEN="fake-token"
  DRY_RUN=0

  gh() {
    cat <<'JSON'
{"workflow_runs": [
  {"name": "Release - Prepare RC", "path": ".github/workflows/release-prepare-rc.yml", "status": "completed", "conclusion": "failure"},
  {"name": "CI Hadoop 3",          "path": ".github/workflows/ci-hadoop3.yml",         "status": "completed", "conclusion": "success"}
]}
JSON
  }
  export -f gh

  run check_github_checks_passed "abc123"
  [ "$status" -eq 0 ]
  [[ "$output" == *"All GitHub checks passed"* ]]
}

@test "check_github_checks_passed: detects CI failures when release workflows are present" {
  export GITHUB_TOKEN="fake-token"
  DRY_RUN=0

  gh() {
    cat <<'JSON'
{"workflow_runs": [
  {"name": "Release - Prepare RC", "path": ".github/workflows/release-prepare-rc.yml", "status": "in_progress", "conclusion": null},
  {"name": "Vector Plugins",       "path": ".github/workflows/vector-plugins.yml",     "status": "completed",   "conclusion": "failure"}
]}
JSON
  }
  export -f gh

  run check_github_checks_passed "abc123"
  [ "$status" -eq 1 ]
  [[ "$output" == *"failed GitHub checks"* ]]
  [[ "$output" == *"Vector Plugins"* ]]
}

@test "check_github_checks_passed: fails when gh api errors" {
  export GITHUB_TOKEN="fake-token"
  DRY_RUN=0

  gh() { return 1; }
  export -f gh

  run check_github_checks_passed "abc123"
  [ "$status" -eq 1 ]
  [[ "$output" == *"Failed to fetch"* ]]
}
