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
# Updates the inlined parquet.thrift and sidecar to a specific parquet-format
# commit or tag.
#
# Usage:
#   update-parquet-thrift.sh <ref>
#
#   <ref> is a full 40-char parquet-format commit SHA, or a parquet-format tag
#   (e.g. apache-parquet-format-2.13.0).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
THRIFT_FILE="${REPO_ROOT}/parquet-format-structures/src/main/thrift/parquet.thrift"
SIDECAR_FILE="${REPO_ROOT}/parquet-format-structures/src/main/thrift/parquet-format.version"

PARQUET_FORMAT_REPO="https://github.com/apache/parquet-format"
PARQUET_FORMAT_RAW="https://raw.githubusercontent.com/apache/parquet-format"
THRIFT_PATH_IN_FORMAT="src/main/thrift/parquet.thrift"

usage() {
  cat <<EOF
Usage: $0 <ref>
  <ref>  a full 40-char parquet-format commit SHA, or a parquet-format tag
         (e.g. apache-parquet-format-2.13.0)
EOF
  exit 1
}

[[ $# -ne 1 ]] && usage

ref="$1"

# Resolve ref to a full commit SHA.
if [[ "$ref" =~ ^[0-9a-f]{40}$ ]]; then
  resolved_sha="$ref"
else
  echo "Resolving ${ref} ..."
  ls_out="$(git ls-remote "$PARQUET_FORMAT_REPO" "$ref" "${ref}^{}")" || {
    echo "ERROR: git ls-remote failed for '${ref}' in ${PARQUET_FORMAT_REPO}" >&2
    exit 1
  }
  if [[ -z "$ls_out" ]]; then
    echo "ERROR: ref '${ref}' not found in ${PARQUET_FORMAT_REPO}" >&2
    exit 1
  fi
  resolved_sha="$(printf '%s\n' "$ls_out" \
    | awk '{ if ($2 ~ /\^\{\}$/) deref=$1; else plain=$1 }
           END { print (deref != "" ? deref : plain) }')"
  if [[ -z "$resolved_sha" ]]; then
    echo "ERROR: could not resolve commit SHA for '${ref}'" >&2
    exit 1
  fi
  echo "  -> commit: ${resolved_sha}"
fi

# Read the old commit from the sidecar for the summary.
old_commit="(none)"
if [[ -f "$SIDECAR_FILE" ]]; then
  old_commit="$(grep '^parquet-format.commit=' "$SIDECAR_FILE" | cut -d= -f2 || true)"
fi

# Fetch upstream parquet.thrift to a temp file. Only overwrite the inlined file after fetch and
# validation succeed, so a failure never overwrites the tracked IDL.
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

echo "Fetching parquet.thrift at ${resolved_sha} ..."
url="${PARQUET_FORMAT_RAW}/${resolved_sha}/${THRIFT_PATH_IN_FORMAT}"
if ! curl -fsSL -o "$tmp" "$url"; then
  echo "ERROR: could not fetch parquet.thrift at ${resolved_sha}" >&2
  echo "       The tracked files were not modified." >&2
  exit 1
fi

if [[ ! -s "$tmp" ]]; then
  echo "ERROR: fetched parquet.thrift is empty" >&2
  echo "       The tracked files were not modified." >&2
  exit 1
fi
if ! grep -q 'namespace java org.apache.parquet.format' "$tmp"; then
  echo "ERROR: fetched file does not look like parquet.thrift (missing namespace declaration)" >&2
  echo "       The tracked files were not modified." >&2
  exit 1
fi

# Write the validated thrift file, then write the sidecar.
mv "$tmp" "$THRIFT_FILE"
chmod 644 "$THRIFT_FILE"
echo "  -> written to ${THRIFT_FILE}"

cat > "$SIDECAR_FILE" <<SIDECAR
# Provenance of the inlined parquet.thrift. Maintained by dev/update-parquet-thrift.sh.
parquet-format.commit=${resolved_sha}
SIDECAR
echo "  -> sidecar updated: ${SIDECAR_FILE}"

# Summary
echo ""
echo "Update complete:"
echo "  old commit: ${old_commit}"
echo "  new commit: ${resolved_sha}"
echo ""
echo "Next steps: rebuild parquet-format-structures and review the diff:"
echo "  ./mvnw -pl parquet-format-structures -am install -DskipTests"
echo "  git diff parquet-format-structures/src/main/thrift/parquet.thrift"
