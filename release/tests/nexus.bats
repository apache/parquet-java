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

# ---- nexus_get_staging_repo_metadata ----

@test "nexus_get_staging_repo_metadata: dry-run sets placeholder values" {
  DRY_RUN=1
  nexus_get_staging_repo_metadata "orgapacheparquet-1234"
  [ "${nexus_repo_profile}" = "org.apache.parquet" ]
  [ "${nexus_repo_state}" = "closed" ]
  [ -n "${nexus_repo_description}" ]
}

@test "nexus_get_staging_repo_metadata: parses real-mode JSON response" {
  DRY_RUN=0
  curl() {
    cat <<'JSON'
{
  "profileName": "org.apache.parquet",
  "type": "closed",
  "description": "Apache Parquet 1.18.0 RC0"
}
JSON
    return 0
  }
  export -f curl
  nexus_get_staging_repo_metadata "orgapacheparquet-1234"
  [ "${nexus_repo_profile}" = "org.apache.parquet" ]
  [ "${nexus_repo_state}" = "closed" ]
  [ "${nexus_repo_description}" = "Apache Parquet 1.18.0 RC0" ]
}

@test "nexus_get_staging_repo_metadata: fails when curl fails" {
  DRY_RUN=0
  curl() { return 22; }
  export -f curl
  run nexus_get_staging_repo_metadata "orgapacheparquet-1234"
  [ "$status" -eq 1 ]
  [[ "$output" == *"Failed to fetch"* ]]
}

# ---- nexus_check_staging_artifact ----

@test "nexus_check_staging_artifact: dry-run skips check" {
  DRY_RUN=1
  run nexus_check_staging_artifact "orgapacheparquet-1234" "1.18.0"
  [ "$status" -eq 0 ]
  [[ "$output" == *"Dry-run"* ]]
  [[ "$output" == *"parquet-common-1.18.0.pom"* ]]
}

@test "nexus_check_staging_artifact: succeeds when artifact present" {
  DRY_RUN=0
  curl() { return 0; }
  export -f curl
  run nexus_check_staging_artifact "orgapacheparquet-1234" "1.18.0"
  [ "$status" -eq 0 ]
}

@test "nexus_check_staging_artifact: fails when artifact missing" {
  DRY_RUN=0
  curl() { return 22; }
  export -f curl
  run nexus_check_staging_artifact "orgapacheparquet-1234" "1.18.0"
  [ "$status" -eq 1 ]
  [[ "$output" == *"not found"* ]]
}

# ---- nexus_verify_staging_repo ----

@test "nexus_verify_staging_repo: dry-run passes without making real calls" {
  DRY_RUN=1
  run nexus_verify_staging_repo "orgapacheparquet-1234" "1.18.0" "0"
  [ "$status" -eq 0 ]
  [[ "$output" == *"verified"* ]]
}

@test "nexus_verify_staging_repo: rejects wrong profile" {
  DRY_RUN=0
  curl() {
    cat <<'JSON'
{"profileName": "org.apache.iceberg", "type": "closed", "description": "Apache Parquet 1.18.0 RC0"}
JSON
    return 0
  }
  export -f curl
  run nexus_verify_staging_repo "orgapacheparquet-1234" "1.18.0" "0"
  [ "$status" -eq 1 ]
  [[ "$output" == *"Profile mismatch"* ]]
}

@test "nexus_verify_staging_repo: rejects non-closed state" {
  DRY_RUN=0
  curl() {
    cat <<'JSON'
{"profileName": "org.apache.parquet", "type": "open", "description": "Apache Parquet 1.18.0 RC0"}
JSON
    return 0
  }
  export -f curl
  run nexus_verify_staging_repo "orgapacheparquet-1234" "1.18.0" "0"
  [ "$status" -eq 1 ]
  [[ "$output" == *"Unexpected state"* ]]
}

@test "nexus_verify_staging_repo: rejects released state" {
  DRY_RUN=0
  curl() {
    cat <<'JSON'
{"profileName": "org.apache.parquet", "type": "released", "description": "Apache Parquet 1.18.0 RC0"}
JSON
    return 0
  }
  export -f curl
  run nexus_verify_staging_repo "orgapacheparquet-1234" "1.18.0" "0"
  [ "$status" -eq 1 ]
  [[ "$output" == *"Unexpected state"* ]]
}

@test "nexus_verify_staging_repo: rejects when artifact missing" {
  DRY_RUN=0
  curl() {
    if [[ "$*" == *"staging/repository"* ]]; then
      cat <<'JSON'
{"profileName": "org.apache.parquet", "type": "closed", "description": "Apache Parquet 1.18.0 RC0"}
JSON
      return 0
    fi
    return 22
  }
  export -f curl
  run nexus_verify_staging_repo "orgapacheparquet-1234" "1.18.0" "0"
  [ "$status" -eq 1 ]
  [[ "$output" == *"not found"* ]]
}

@test "nexus_verify_staging_repo: rejects description mismatch by default" {
  DRY_RUN=0
  curl() {
    if [[ "$*" == *"staging/repository"* ]]; then
      cat <<'JSON'
{"profileName": "org.apache.parquet", "type": "closed", "description": "Apache Parquet 1.18.0 RC1"}
JSON
    fi
    return 0
  }
  export -f curl
  run nexus_verify_staging_repo "orgapacheparquet-1234" "1.18.0" "0"
  [ "$status" -eq 1 ]
  [[ "$output" == *"Description mismatch"* ]]
  [[ "$output" == *"--allow-description-mismatch"* ]]
}

@test "nexus_verify_staging_repo: allows description mismatch with flag" {
  DRY_RUN=0
  curl() {
    if [[ "$*" == *"staging/repository"* ]]; then
      cat <<'JSON'
{"profileName": "org.apache.parquet", "type": "closed", "description": "Apache Parquet 1.18.0 RC1"}
JSON
    fi
    return 0
  }
  export -f curl
  run nexus_verify_staging_repo "orgapacheparquet-1234" "1.18.0" "0" "1"
  [ "$status" -eq 0 ]
  [[ "$output" == *"Description mismatch"* ]]
  [[ "$output" == *"Continuing despite"* ]]
}

@test "nexus_verify_staging_repo: passes when everything matches" {
  DRY_RUN=0
  curl() {
    if [[ "$*" == *"staging/repository"* ]]; then
      cat <<'JSON'
{"profileName": "org.apache.parquet", "type": "closed", "description": "Apache Parquet 1.18.0 RC0"}
JSON
    fi
    return 0
  }
  export -f curl
  run nexus_verify_staging_repo "orgapacheparquet-1234" "1.18.0" "0"
  [ "$status" -eq 0 ]
  [[ "$output" == *"verified"* ]]
}
