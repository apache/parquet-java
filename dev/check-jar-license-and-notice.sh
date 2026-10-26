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

set -euo pipefail

# ASF release policy requires every distributed JAR to include LICENSE and NOTICE
# in META-INF: https://www.apache.org/legal/release-policy.html#licensing-documentation
# This script expects the JARs to have already been built.
readonly -a required_entries=(META-INF/LICENSE META-INF/NOTICE)

search_roots=("$@")
if [[ ${#search_roots[@]} -eq 0 ]]; then
  search_roots=(.)
fi

should_check_jar() {
  local jar_file=$1
  local jar_name

  # Only check JARs directly under a module's target directory.
  [[ $(basename "$(dirname "$jar_file")") == "target" ]] || return 1

  jar_name=$(basename "$jar_file")
  case "$jar_name" in
    original-*.jar)
      # Maven Shade backups are build intermediates and are not deployed.
      return 1
      ;;
    parquet-benchmarks.jar)
      # This local-only JMH uber-JAR is not deployed. Maven publishes the
      # versioned parquet-benchmarks artifact instead.
      return 1
      ;;
  esac

  return 0
}

# Populates the global jar_files array.
find_jar_candidates() {
  local jar_paths_file=$1
  shift
  local jar_file

  if ! find "$@" -type f -name "*.jar" -print0 >"$jar_paths_file"; then
    printf '%s\n' "Unable to complete the search for published JAR candidates." >&2
    return 1
  fi

  jar_files=()
  while IFS= read -r -d '' jar_file; do
    if should_check_jar "$jar_file"; then
      jar_files+=("$jar_file")
    fi
  done <"$jar_paths_file"
}

check_jar() {
  local jar_file=$1
  local jar_listing_file=$2
  local has_missing_entry=0
  local required_entry

  if ! jar tf "$jar_file" >"$jar_listing_file"; then
    printf 'Unable to read JAR: %s\n' "$jar_file" >&2
    return 1
  fi

  for required_entry in "${required_entries[@]}"; do
    if ! grep -Fqx "$required_entry" "$jar_listing_file"; then
      printf 'Missing %s: %s\n' "$required_entry" "$jar_file" >&2
      has_missing_entry=1
    fi
  done

  return "$has_missing_entry"
}

jar_paths_file=$(mktemp)
jar_listing_file=$(mktemp)
trap 'rm -f "$jar_paths_file" "$jar_listing_file"' EXIT

jar_files=()
find_jar_candidates "$jar_paths_file" "${search_roots[@]}"

jar_count=${#jar_files[@]}
if [[ $jar_count -eq 0 ]]; then
  printf '%s\n' "No published JAR candidates found directly under module target directories." >&2
  exit 1
fi

has_failures=0
verified_count=0
for jar_file in "${jar_files[@]}"; do
  if check_jar "$jar_file" "$jar_listing_file"; then
    verified_count=$((verified_count + 1))
  else
    has_failures=1
  fi
done

printf 'Successfully verified %d of %d JAR(s).\n' "$verified_count" "$jar_count"
exit "$has_failures"
