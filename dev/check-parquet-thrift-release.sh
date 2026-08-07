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

# Verifies that the parquet-format.version sidecar file is tracking a released
# parquet-format version and that the inlined parquet.thrift byte-matches the
# upstream file at that version.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
THRIFT_FILE="${REPO_ROOT}/parquet-format-structures/src/main/thrift/parquet.thrift"
SIDECAR_FILE="${REPO_ROOT}/parquet-format-structures/src/main/thrift/parquet-format.version"

# shellcheck source=parquet-thrift-lib.sh
source "${SCRIPT_DIR}/parquet-thrift-lib.sh"

if [[ ! -f "$SIDECAR_FILE" ]]; then
  echo "ERROR: sidecar not found: ${SIDECAR_FILE}" >&2
  exit 1
fi

if [[ ! -f "$THRIFT_FILE" ]]; then
  echo "ERROR: inlined thrift not found: ${THRIFT_FILE}" >&2
  exit 1
fi

version="$(grep '^parquet-format.version=' "$SIDECAR_FILE" | cut -d= -f2 || true)"
commit="$(grep '^parquet-format.commit=' "$SIDECAR_FILE" | cut -d= -f2 || true)"

# Require a valid X.Y.Z release version
if [[ -z "$version" ]] || ! [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "ERROR: inlined parquet.thrift is not from a released parquet-format version." >&2
  echo "  parquet-format.version = '${version}'" >&2
  echo "" >&2
  echo "A parquet-java release must use a released parquet-format IDL." >&2
  echo "Run: dev/update-parquet-thrift.sh --version <X.Y.Z>" >&2
  exit 1
fi

echo "Checking inlined parquet.thrift against parquet-format ${version} (commit ${commit}) ..."

# Fetch the canonical IDL from the release tag. Never writes to tracked files.
# Fails closed: a network/git failure or an empty file all exit non-zero.
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

if ! fetch_thrift "apache-parquet-format-${version}" "$tmp"; then
  echo "ERROR: failed to fetch parquet.thrift for tag apache-parquet-format-${version}" >&2
  echo "Cannot verify inlined IDL — failing closed." >&2
  exit 1
fi

if [[ ! -s "$tmp" ]]; then
  echo "ERROR: fetched parquet.thrift is empty for tag apache-parquet-format-${version}" >&2
  echo "Cannot verify inlined IDL — failing closed." >&2
  exit 1
fi

if diff -u "$tmp" "$THRIFT_FILE"; then
  echo "OK: inlined parquet.thrift matches parquet-format ${version}."
  exit 0
else
  echo "" >&2
  echo "ERROR: inlined parquet.thrift differs from parquet-format ${version}." >&2
  echo "Run: dev/update-parquet-thrift.sh --version ${version}" >&2
  exit 1
fi
