#!/bin/bash
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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIBS_DIR="${SCRIPT_DIR}/../libs"

source "${LIBS_DIR}/_constants.sh"
source "${LIBS_DIR}/_log.sh"
source "${LIBS_DIR}/_exec.sh"
source "${LIBS_DIR}/_version.sh"
source "${LIBS_DIR}/_nexus.sh"
source "${LIBS_DIR}/_svn.sh"

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------
function usage {
  cat <<EOF
Usage: $0 <version> <rc-num> <staging-repo-id> [--allow-description-mismatch]

Cancel a release candidate after a failed vote.

Arguments:
  version               Release version (e.g., 1.18.0)
  rc-num                RC number to cancel (e.g., 0)
  staging-repo-id       Nexus staging repository ID (e.g., orgapacheparquet-1234)

Options:
  --allow-description-mismatch
                        Bypass the staging-repo description check (for recovery scenarios)

Before dropping the staging repo, this script verifies that it belongs
to org.apache.parquet, is in 'closed' state, contains
${NEXUS_VERIFY_ARTIFACT_ID:-parquet-common}-<version>.pom, and has a
description matching "Apache Parquet <version> RC<num>".

Environment variables:
  DRY_RUN               Set to 0 for real execution (default: 1)
  NEXUS_USERNAME        Apache Nexus username
  NEXUS_PASSWORD        Apache Nexus password
  SVN_USERNAME          SVN username for dist.apache.org
  SVN_PASSWORD          SVN password

Example:
  DRY_RUN=1 $0 1.18.0 0 orgapacheparquet-1234
EOF
  exit "${1:-0}"
}

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
version=""
rc_num=""
staging_repo_id=""
allow_description_mismatch=0
positional=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --allow-description-mismatch)
      allow_description_mismatch=1
      shift
      ;;
    --help|-h)
      usage 0
      ;;
    -*)
      print_error "Unknown option: $1"
      usage 1
      ;;
    *)
      positional+=("$1")
      shift
      ;;
  esac
done

if [[ ${#positional[@]} -lt 3 ]]; then
  print_error "Expected 3 positional arguments (version, rc-num, staging-repo-id), got ${#positional[@]}"
  usage 1
fi

version="${positional[0]}"
rc_num="${positional[1]}"
staging_repo_id="${positional[2]}"

# ---------------------------------------------------------------------------
# Validate inputs
# ---------------------------------------------------------------------------
step_summary "## Release Candidate Cancellation"
step_summary ""

if [[ ${DRY_RUN:-1} -eq 1 ]]; then
  step_summary "> **DRY RUN** -- no changes will be made"
  step_summary ""
fi

if ! validate_and_extract_version "${version}"; then
  print_error "Invalid version format: '${version}'"
  exit 1
fi

if ! [[ "${rc_num}" =~ ^[0-9]+$ ]]; then
  print_error "Invalid RC number: '${rc_num}'. Expected a non-negative integer."
  exit 1
fi

if ! [[ "${staging_repo_id}" =~ ^[a-zA-Z][a-zA-Z0-9._-]*$ ]]; then
  print_error "Invalid staging repository ID: '${staging_repo_id}'. Expected alphanumeric with dots/hyphens (e.g., orgapacheparquet-1234)."
  exit 1
fi

rc_tag="${TAG_PREFIX}${version}-rc${rc_num}"

step_summary "| Parameter | Value |"
step_summary "| --- | --- |"
step_summary "| Version | \`${version}\` |"
step_summary "| RC tag | \`${rc_tag}\` |"
step_summary "| Staging repo | \`${staging_repo_id}\` |"

# ---------------------------------------------------------------------------
# Step 0: Verify staging repository before any destructive action
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Staging Repository Verification"

if ! nexus_verify_staging_repo "${staging_repo_id}" "${version}" "${rc_num}" "${allow_description_mismatch}"; then
  step_summary "Staging repository verification: **FAILED**"
  exit 1
fi
step_summary "Staging repository \`${staging_repo_id}\` verified"

# ---------------------------------------------------------------------------
# Step 1: Drop Nexus staging repo
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Nexus Cleanup"

nexus_drop_staging_repo "${staging_repo_id}" "Cancel Apache Parquet ${version} RC${rc_num}"

step_summary "Dropped staging repository \`${staging_repo_id}\`"

# ---------------------------------------------------------------------------
# Step 2: Delete SVN artifacts from dist/dev
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### SVN Cleanup"

dev_url="${APACHE_DIST_URL}${APACHE_DIST_DEV_PATH}/${rc_tag}"

svn_remove_rc "${version}" "${rc_num}"
step_summary "Removed \`${dev_url}\`"

# ---------------------------------------------------------------------------
# Step 3: Generate vote failure email
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Vote Failure Email"
step_summary ""
step_summary '```'
step_summary "Subject: [RESULT][VOTE] Release Apache Parquet ${version} RC${rc_num}"
step_summary ""
step_summary "Hello everyone,"
step_summary ""
step_summary "Thanks to all who participated in the vote for Release Apache Parquet ${version} (rc${rc_num})."
step_summary ""
step_summary "The vote failed due to [REASON - TO BE FILLED BY RELEASE MANAGER]."
step_summary ""
step_summary "A new release candidate will be proposed soon once the issues are addressed."
step_summary ""
step_summary "Thanks,"
step_summary '```'

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
step_summary ""
step_summary "---"
step_summary "### Summary"
step_summary ""
step_summary "| Step | Status |"
step_summary "| --- | --- |"
step_summary "| Nexus staging repo | dropped |"
step_summary "| SVN dist/dev | deleted |"
step_summary "| Failure email | generated |"

print_success "Release candidate ${rc_tag} cancelled successfully."
