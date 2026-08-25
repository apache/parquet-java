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
# commit or release.
#
# Usage:
#   update-parquet-thrift.sh <full-40-char-commit-hash>  -- POC: sets version=UNRELEASED
#   update-parquet-thrift.sh --version <X.Y.Z>           -- release sync: derives commit from tag

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
THRIFT_FILE="${REPO_ROOT}/parquet-format-structures/src/main/thrift/parquet.thrift"
SIDECAR_FILE="${REPO_ROOT}/parquet-format-structures/src/main/thrift/parquet-format.version"

# shellcheck source=parquet-thrift-lib.sh
source "${SCRIPT_DIR}/parquet-thrift-lib.sh"

usage() {
  cat <<EOF
Usage:
  $0 <full-40-char-commit-hash>  POC sync — fetch parquet.thrift at that commit; version=UNRELEASED
  $0 --version <X.Y.Z>           Release sync — resolve tag apache-parquet-format-<X.Y.Z>

Only one of a commit hash or --version may be given, not both.
Commit mode requires a full 40-character sha (short shas cannot be fetched from a remote).
EOF
  exit 1
}

if [[ $# -eq 0 ]]; then
  usage
fi

mode=""
commit_arg=""
version_arg=""

if [[ "$1" == "--version" ]]; then
  [[ $# -ne 2 ]] && usage
  mode="version"
  version_arg="$2"
elif [[ "$1" == --* ]]; then
  usage
else
  [[ $# -ne 1 ]] && usage
  mode="commit"
  commit_arg="$1"
fi

# Validate arguments
if [[ "$mode" == "version" ]]; then
  if ! [[ "$version_arg" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "ERROR: version must be X.Y.Z (e.g. 2.13.0), got: $version_arg" >&2
    exit 1
  fi
else
  # Full 40-char sha required — short shas cannot be fetched from a remote (git exits 128).
  if ! [[ "$commit_arg" =~ ^[0-9a-f]{40}$ ]]; then
    echo "ERROR: commit hash must be exactly 40 hex chars, got: $commit_arg" >&2
    echo "       Obtain the full sha with: git ls-remote ${PARQUET_FORMAT_REPO} HEAD" >&2
    exit 1
  fi
fi

# Resolve the commit sha to use
resolved_sha=""
resolved_version=""

if [[ "$mode" == "version" ]]; then
  echo "Resolving tag apache-parquet-format-${version_arg} ..."
  resolved_sha="$(resolve_version_to_commit "$version_arg")" || {
    rc=$?
    if [[ $rc -eq 1 ]]; then
      echo "ERROR: tag apache-parquet-format-${version_arg} not found in ${PARQUET_FORMAT_REPO}" >&2
      echo "       Check that the version exists: git ls-remote --tags ${PARQUET_FORMAT_REPO}" >&2
    else
      echo "ERROR: git/network failure resolving tag apache-parquet-format-${version_arg}" >&2
    fi
    exit 1
  }
  resolved_version="$version_arg"
  echo "  -> commit: $resolved_sha"
else
  resolved_sha="$commit_arg"
  resolved_version="UNRELEASED"
fi

# Read the old commit from the sidecar (for the summary)
old_commit="(none)"
if [[ -f "$SIDECAR_FILE" ]]; then
  old_commit="$(grep '^parquet-format.commit=' "$SIDECAR_FILE" | cut -d= -f2 || true)"
fi

# Fetch upstream parquet.thrift to a temp file. Only overwrite the inlined file after fetch and
# validation succeed, so a failure never deletes the inlined version.
tmp_thrift="$(mktemp)"
trap 'rm -f "$tmp_thrift"' EXIT

echo "Fetching parquet.thrift at ${resolved_sha} ..."
if ! fetch_thrift "$resolved_sha" "$tmp_thrift"; then
  echo "ERROR: could not fetch parquet.thrift at ${resolved_sha}" >&2
  echo "       The tracked files were not modified." >&2
  exit 1
fi

if [[ ! -s "$tmp_thrift" ]]; then
  echo "ERROR: fetched parquet.thrift is empty" >&2
  echo "       The tracked files were not modified." >&2
  exit 1
fi
if ! grep -q 'namespace java org.apache.parquet.format' "$tmp_thrift"; then
  echo "ERROR: fetched file does not look like parquet.thrift (missing namespace declaration)" >&2
  echo "       The tracked files were not modified." >&2
  exit 1
fi

# Commit the validated thrift file, then write the sidecar.
mv "$tmp_thrift" "$THRIFT_FILE"
chmod 644 "$THRIFT_FILE"
echo "  -> written to ${THRIFT_FILE}"

cat > "$SIDECAR_FILE" <<SIDECAR
# Provenance of the inlined parquet.thrift. Maintained by dev/update-parquet-thrift.sh.
parquet-format.commit=${resolved_sha}
parquet-format.version=${resolved_version}
SIDECAR
echo "  -> sidecar updated: ${SIDECAR_FILE}"

# Summary
echo ""
echo "Update complete:"
echo "  old commit: ${old_commit}"
echo "  new commit: ${resolved_sha}"
if [[ "$resolved_version" == "UNRELEASED" ]]; then
  echo "  version:    UNRELEASED (POC sync — run --version <ver> for a release sync)"
else
  echo "  version:    ${resolved_version} (release sync)"
fi
echo ""
echo "Next steps: rebuild parquet-format-structures and review the diff:"
echo "  ./mvnw -pl parquet-format-structures -am install -DskipTests"
echo "  git diff parquet-format-structures/src/main/thrift/parquet.thrift"
