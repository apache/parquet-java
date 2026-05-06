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
  source "${LIBS_DIR}/_nexus.sh"
  export NEXUS_USERNAME="testuser"
  export NEXUS_PASSWORD="testpass"
}

# ---- nexus_close_staging_repo ----

@test "nexus_close_staging_repo: dry-run does not call curl" {
  DRY_RUN=1
  run nexus_close_staging_repo "orgapacheparquet-1234" "test close"
  [ "$status" -eq 0 ]
  [[ "$output" == *"Dry-run"* ]]
  [[ "$output" == *"close"* ]]
}

@test "nexus_close_staging_repo: constructs correct URL in dry-run" {
  DRY_RUN=1
  run nexus_close_staging_repo "orgapacheparquet-1234"
  [[ "$output" == *"staging/bulk/close"* ]]
}

# ---- nexus_release_staging_repo ----

@test "nexus_release_staging_repo: dry-run includes promote URL" {
  DRY_RUN=1
  run nexus_release_staging_repo "orgapacheparquet-1234"
  [[ "$output" == *"staging/bulk/promote"* ]]
}

# ---- nexus_drop_staging_repo ----

@test "nexus_drop_staging_repo: dry-run includes drop URL" {
  DRY_RUN=1
  run nexus_drop_staging_repo "orgapacheparquet-1234"
  [[ "$output" == *"staging/bulk/drop"* ]]
}

# ---- _nexus_bulk_action ----

@test "_nexus_bulk_action: includes repo ID in payload" {
  DRY_RUN=1
  run _nexus_bulk_action "close" "orgapacheparquet-5678" "test"
  [[ "$output" == *"orgapacheparquet-5678"* ]]
}

@test "_nexus_bulk_action: uses correct base URL" {
  DRY_RUN=1
  NEXUS_BASE_URL="https://example.com/nexus"
  run _nexus_bulk_action "drop" "repo-123" "test"
  [[ "$output" == *"example.com/nexus/staging/bulk/drop"* ]]
}

# ---- nexus_find_open_staging_repo ----

@test "nexus_find_open_staging_repo: dry-run returns placeholder" {
  DRY_RUN=1
  nexus_find_open_staging_repo "org.apache.parquet"
  [ "$staging_repo_id" = "DRY-RUN-REPO-ID" ]
}

# ---- real-mode tests with mocked curl ----

@test "nexus_close_staging_repo: real mode calls curl with correct args" {
  DRY_RUN=0
  curl() {
    echo "CURL_ARGS: $*" >&2
    return 0
  }
  export -f curl
  run nexus_close_staging_repo "orgapacheparquet-9999" "test desc"
  [ "$status" -eq 0 ]
  [[ "$output" == *"staging/bulk/close"* ]]
  [[ "$output" == *"orgapacheparquet-9999"* ]]
}

@test "nexus_drop_staging_repo: real mode calls curl with auth" {
  DRY_RUN=0
  curl() {
    echo "CURL_ARGS: $*" >&2
    return 0
  }
  export -f curl
  run nexus_drop_staging_repo "orgapacheparquet-1111"
  [ "$status" -eq 0 ]
  [[ "$output" == *"staging/bulk/drop"* ]]
}
