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

# Shared helpers for managing the inlined parquet.thrift file and its relation to the upstream
# apache/parquet-format repository.

PARQUET_FORMAT_REPO="https://github.com/apache/parquet-format"
PARQUET_FORMAT_RAW="https://raw.githubusercontent.com/apache/parquet-format"
THRIFT_PATH_IN_FORMAT="src/main/thrift/parquet.thrift"

# Resolve an apache/parquet-format release version (X.Y.Z) to its full commit SHA using git
# ls-remote. Echoes the SHA and returns 0 on success, returns 1 if the tag does not exist, and
# returns the exit code of any other commands on failure.
resolve_version_to_commit() {
  local ver="$1" sha ls_out ls_rc
  ls_out="$(git ls-remote --tags "$PARQUET_FORMAT_REPO" \
              "refs/tags/apache-parquet-format-${ver}" \
              "refs/tags/apache-parquet-format-${ver}^{}")" || { ls_rc=$?; return $ls_rc; }
  sha="$(printf '%s\n' "$ls_out" \
         | awk '{ if ($2 ~ /\^\{\}$/) deref=$1; else plain=$1 }
                END { print (deref != "" ? deref : plain) }')"
  [[ -n "$sha" ]] || return 1
  printf '%s\n' "$sha"
}

# Fetch the parquet.thrift IDL from the parquet-format repo at the given commit
# sha or tag, writing output to $2. Reads from raw.githubusercontent.com, which
# serves any commit sha or tag and is not subject to the GitHub REST API rate
# limit. Returns 0 on success, 1 on any failure.
# The caller is responsible for providing a temp path for $2 so tracked source
# files are never overwritten on failure.

# Fetch the canonical parquet.thrift IDL from the parquet-format repo at a given commit SHA or tag
# (supplied as $1) and write it to the destination specified by $2.
fetch_thrift() {
  local ref="$1" dest="$2" url attempt
  url="${PARQUET_FORMAT_RAW}/${ref}/${THRIFT_PATH_IN_FORMAT}"
  for attempt in 1 2 3; do
    if curl -fsSL -o "$dest" "$url" && [[ -s "$dest" ]]; then
      return 0
    fi
    if [[ "$attempt" -eq 3 ]]; then
      echo "ERROR: failed to fetch ${url} after ${attempt} attempts" >&2
      return 1
    fi
    sleep $((attempt * 2))
  done
}
