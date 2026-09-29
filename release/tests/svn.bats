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
  source "${LIBS_DIR}/_svn.sh"
  export SVN_USERNAME="testuser"
  export SVN_PASSWORD="testpass"
}

# ---- svn_stage_rc ----

@test "svn_stage_rc: dry-run does not call svn" {
  DRY_RUN=1
  local f="${BATS_TEST_TMPDIR}/parquet-1.18.0.tar.gz"
  echo "fake tarball" > "$f"
  run svn_stage_rc "1.18.0" "0" "$f"
  [ "$status" -eq 0 ]
  [[ "$output" == *"Dry-run"* ]]
  [[ "$output" == *"apache-parquet-1.18.0-rc0"* ]]
}

@test "svn_stage_rc: rejects empty file list" {
  DRY_RUN=1
  run svn_stage_rc "1.18.0" "0"
  [ "$status" -ne 0 ]
  [[ "$output" == *"no files to stage"* ]]
}

@test "svn_stage_rc: dry-run does not write to repo root tmp/" {
  DRY_RUN=1
  cd "${BATS_TEST_TMPDIR}"
  local f="${BATS_TEST_TMPDIR}/x.tar.gz"
  echo x > "$f"
  run svn_stage_rc "1.18.0" "0" "$f"
  [ "$status" -eq 0 ]
  [ ! -d "${BATS_TEST_TMPDIR}/tmp" ]
}

# ---- svn_promote_rc_to_release ----

@test "svn_promote_rc_to_release: dry-run shows mv command" {
  DRY_RUN=1
  run svn_promote_rc_to_release "1.18.0" "2"
  [ "$status" -eq 0 ]
  [[ "$output" == *"svn mv"* ]]
  [[ "$output" == *"apache-parquet-1.18.0-rc2"* ]]
  [[ "$output" == *"apache-parquet-1.18.0"* ]]
  [[ "$output" == *"/dev/parquet"* ]]
  [[ "$output" == *"/release/parquet"* ]]
}

@test "svn_promote_rc_to_release: real mode invokes svn mv" {
  DRY_RUN=0
  svn() { echo "SVN: $*"; return 0; }
  export -f svn
  run svn_promote_rc_to_release "1.18.0" "0"
  [ "$status" -eq 0 ]
  [[ "$output" == *"SVN: mv"* ]]
}

# ---- svn_remove_rc ----

@test "svn_remove_rc: dry-run shows rm command" {
  DRY_RUN=1
  run svn_remove_rc "1.18.0" "1"
  [ "$status" -eq 0 ]
  [[ "$output" == *"svn rm"* ]]
  [[ "$output" == *"apache-parquet-1.18.0-rc1"* ]]
  [[ "$output" == *"/dev/parquet"* ]]
}

@test "svn_remove_rc: real mode skips rm if path does not exist" {
  DRY_RUN=0
  svn() {
    if [[ "$1" == "ls" ]]; then return 1; fi
    echo "UNEXPECTED svn $*"; return 99
  }
  export -f svn
  run svn_remove_rc "1.18.0" "0"
  [ "$status" -eq 0 ]
  [[ "$output" == *"already deleted"* ]] || [[ "$output" == *"not found"* ]]
}

@test "svn_remove_rc: real mode invokes svn rm when path exists" {
  DRY_RUN=0
  svn() {
    if [[ "$1" == "ls" ]]; then return 0; fi
    echo "SVN: $*"; return 0
  }
  export -f svn
  run svn_remove_rc "1.18.0" "0"
  [ "$status" -eq 0 ]
  [[ "$output" == *"SVN: rm"* ]]
}

# ---- svn_list_old_releases ----
#
# Policy under test: cleanup is scoped to *older patch releases on the same
# minor branch*. Releasing 1.9.2 cleans up 1.9.0 and 1.9.1; it must never
# touch other minors (1.8.x, 1.10.x), the kept version itself, or any
# higher patch on the same minor.

@test "svn_list_old_releases: dry-run returns empty without calling svn" {
  DRY_RUN=1
  run svn_list_old_releases "1.18.0"
  [ "$status" -eq 0 ]
  # No actual entries, just the dry-run banner on stderr
  [ -z "$(echo "$output" | grep -v Dry-run | grep -v WOULD)" ]
}

@test "svn_list_old_releases: returns older patches on the same minor only" {
  DRY_RUN=0
  svn() {
    if [[ "$1" == "list" ]]; then
      printf '%s\n' \
        'apache-parquet-1.8.3/' \
        'apache-parquet-1.9.0/' \
        'apache-parquet-1.9.1/' \
        'apache-parquet-1.9.2/' \
        'apache-parquet-1.10.0/' \
        'KEYS' 'README'
      return 0
    fi
    return 99
  }
  export -f svn
  run svn_list_old_releases "1.9.2"
  [ "$status" -eq 0 ]
  [[ "$output" == *"apache-parquet-1.9.0"* ]]
  [[ "$output" == *"apache-parquet-1.9.1"* ]]
  # Same-minor current and other-minor entries must not be returned.
  [[ "$output" != *"apache-parquet-1.9.2"* ]]
  [[ "$output" != *"apache-parquet-1.8.3"* ]]
  [[ "$output" != *"apache-parquet-1.10.0"* ]]
  [[ "$output" != *"KEYS"* ]]
  [[ "$output" != *"README"* ]]
}

@test "svn_list_old_releases: never touches a higher patch on the same minor" {
  DRY_RUN=0
  svn() {
    if [[ "$1" == "list" ]]; then
      printf '%s\n' \
        'apache-parquet-1.9.1/' \
        'apache-parquet-1.9.2/' \
        'apache-parquet-1.9.3/'
      return 0
    fi
    return 99
  }
  export -f svn
  run svn_list_old_releases "1.9.2"
  [ "$status" -eq 0 ]
  [[ "$output" == *"apache-parquet-1.9.1"* ]]
  [[ "$output" != *"apache-parquet-1.9.2"* ]]
  [[ "$output" != *"apache-parquet-1.9.3"* ]]
}

@test "svn_list_old_releases: returns empty when releasing the .0 of a minor" {
  DRY_RUN=0
  svn() {
    if [[ "$1" == "list" ]]; then
      printf '%s\n' \
        'apache-parquet-1.8.5/' \
        'apache-parquet-1.9.0/' \
        'apache-parquet-1.10.0/'
      return 0
    fi
    return 99
  }
  export -f svn
  run svn_list_old_releases "1.9.0"
  [ "$status" -eq 0 ]
  [ -z "$(echo "$output" | grep -v '^$')" ]
}

@test "svn_list_old_releases: ignores rc and otherwise-malformed siblings" {
  DRY_RUN=0
  svn() {
    if [[ "$1" == "list" ]]; then
      printf '%s\n' \
        'apache-parquet-1.9.0/' \
        'apache-parquet-1.9.1-rc0/' \
        'apache-parquet-1.9.x/' \
        'apache-parquet-format-2.10.0/' \
        'apache-parquet-cpp-1.5.0/'
      return 0
    fi
    return 99
  }
  export -f svn
  run svn_list_old_releases "1.9.2"
  [ "$status" -eq 0 ]
  [[ "$output" == *"apache-parquet-1.9.0"* ]]
  # Only the bare X.Y.Z/ sibling on the same minor counts.
  [[ "$output" != *"rc0"* ]]
  [[ "$output" != *"1.9.x"* ]]
  [[ "$output" != *"format"* ]]
  [[ "$output" != *"cpp"* ]]
}

@test "svn_list_old_releases: handles double-digit patch numbers numerically" {
  DRY_RUN=0
  svn() {
    if [[ "$1" == "list" ]]; then
      printf '%s\n' \
        'apache-parquet-1.9.2/' \
        'apache-parquet-1.9.9/' \
        'apache-parquet-1.9.10/'
      return 0
    fi
    return 99
  }
  export -f svn
  run svn_list_old_releases "1.9.10"
  [ "$status" -eq 0 ]
  [[ "$output" == *"apache-parquet-1.9.2"* ]]
  [[ "$output" == *"apache-parquet-1.9.9"* ]]
  [[ "$output" != *"apache-parquet-1.9.10"* ]]
}

@test "svn_list_old_releases: rejects an invalid version-to-keep" {
  DRY_RUN=0
  run svn_list_old_releases "1.9"
  [ "$status" -ne 0 ]
  [[ "$output" == *"invalid version"* ]]
}

@test "svn_list_old_releases: returns empty when only the kept version exists" {
  DRY_RUN=0
  svn() {
    if [[ "$1" == "list" ]]; then printf 'apache-parquet-1.18.0/\n'; return 0; fi
    return 99
  }
  export -f svn
  run svn_list_old_releases "1.18.0"
  [ "$status" -eq 0 ]
  [[ "$output" != *"apache-parquet"* ]]
}

@test "svn_list_old_releases: fails when svn list errors" {
  DRY_RUN=0
  svn() {
    if [[ "$1" == "list" ]]; then echo "svn: E170013"; return 1; fi
    return 99
  }
  export -f svn
  run svn_list_old_releases "1.18.0"
  [ "$status" -eq 1 ]
  [[ "$output" == *"Failed to list"* ]]
}

# ---- svn_remove_release ----

@test "svn_remove_release: dry-run shows rm command" {
  DRY_RUN=1
  run svn_remove_release "apache-parquet-1.16.0" "1.18.0"
  [ "$status" -eq 0 ]
  [[ "$output" == *"svn rm"* ]]
  [[ "$output" == *"apache-parquet-1.16.0"* ]]
  [[ "$output" == *"superseded by 1.18.0"* ]]
}

@test "svn_remove_release: real mode invokes svn rm with commit message" {
  DRY_RUN=0
  svn() { echo "SVN: $*"; return 0; }
  export -f svn
  run svn_remove_release "apache-parquet-1.16.0" "1.18.0"
  [ "$status" -eq 0 ]
  [[ "$output" == *"SVN: rm"* ]]
  [[ "$output" == *"apache-parquet-1.16.0"* ]]
  [[ "$output" == *"superseded by 1.18.0"* ]]
}
