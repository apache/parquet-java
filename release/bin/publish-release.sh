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
Usage: $0 <version> <staging-repo-id> [--rc <num>] [--allow-description-mismatch]

Publish a release after the vote passes.

Arguments:
  version               Release version (e.g., 1.18.0)
  staging-repo-id       Nexus staging repository ID (e.g., orgapacheparquet-1234)

Options:
  --rc <num>            RC number that passed the vote (default: auto-detect latest)
  --allow-description-mismatch
                        Bypass the staging-repo description check (for recovery scenarios)
  --help                Show this help

Before any destructive action, this script verifies that the staging repo
belongs to org.apache.parquet, is in 'closed' state, contains
${NEXUS_VERIFY_ARTIFACT_ID:-parquet-common}-<version>.pom, and has a
description matching "Apache Parquet <version> RC<num>".

The next development version is auto-computed by incrementing the patch
version (e.g., 1.18.0 -> 1.18.1-SNAPSHOT).

Environment variables:
  DRY_RUN               Set to 0 for real execution (default: 1)
  NEXUS_USERNAME        Apache Nexus username
  NEXUS_PASSWORD        Apache Nexus password
  SVN_USERNAME          SVN username for dist.apache.org
  SVN_PASSWORD          SVN password
  GITHUB_TOKEN          GitHub token for release creation

Example:
  DRY_RUN=1 $0 1.18.0 orgapacheparquet-1234
  DRY_RUN=1 $0 1.18.0 orgapacheparquet-1234 --rc 2
EOF
  exit "${1:-0}"
}

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
version=""
staging_repo_id=""
rc_num=""
allow_description_mismatch=0
positional=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rc)
      if [[ -z "${2:-}" ]]; then
        print_error "--rc requires a value"
        usage 1
      fi
      rc_num="$2"
      shift 2
      ;;
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

if [[ ${#positional[@]} -lt 2 ]]; then
  print_error "Expected 2 positional arguments (version, staging-repo-id), got ${#positional[@]}"
  usage 1
fi

version="${positional[0]}"
staging_repo_id="${positional[1]}"

# ---------------------------------------------------------------------------
# Validate inputs
# ---------------------------------------------------------------------------
step_summary "## Release Publication"
step_summary ""

if [[ ${DRY_RUN:-1} -eq 1 ]]; then
  step_summary "> **DRY RUN** -- no changes will be made"
  step_summary ""
fi

if ! validate_and_extract_version "${version}"; then
  print_error "Invalid version format: '${version}'"
  exit 1
fi

next_dev_version="${major}.${minor}.$(( patch + 1 ))"

if ! [[ "${staging_repo_id}" =~ ^[a-zA-Z][a-zA-Z0-9._-]*$ ]]; then
  print_error "Invalid staging repository ID: '${staging_repo_id}'. Expected alphanumeric with dots/hyphens (e.g., orgapacheparquet-1234)."
  exit 1
fi

if [[ -z "${rc_num}" ]]; then
  print_info "No RC number specified, auto-detecting latest RC for ${version}..."
  if ! find_latest_rc_number "${version}"; then
    exit 1
  fi
  rc_num="${latest_rc_number}"
  print_info "Auto-detected latest RC: rc${rc_num}"
else
  if ! [[ "${rc_num}" =~ ^[0-9]+$ ]]; then
    print_error "Invalid RC number: '${rc_num}'. Expected a non-negative integer."
    exit 1
  fi

  if find_latest_rc_number "${version}" 2>/dev/null; then
    if [[ "${rc_num}" -ne "${latest_rc_number}" ]]; then
      print_error "RC${rc_num} is not the latest RC for ${version}. Latest is rc${latest_rc_number}."
      print_error "Publishing an older RC is likely a mistake. If intentional, delete the newer RC tags first."
      exit 1
    fi
  fi
fi

rc_tag="${TAG_PREFIX}${version}-rc${rc_num}"
final_tag="${TAG_PREFIX}${version}"

if ! git rev-parse "${rc_tag}" >/dev/null 2>&1; then
  print_error "RC tag ${rc_tag} does not exist"
  exit 1
fi

rc_commit=$(git rev-list -1 "${rc_tag}")
current_commit=$(git rev-parse HEAD)

if [[ "${current_commit}" != "${rc_commit}" ]]; then
  print_error "Current HEAD (${current_commit}) does not match RC tag ${rc_tag} (${rc_commit})"
  print_error "The release branch has commits beyond the voted RC. Either reset the branch or create a new RC."
  exit 1
fi

if git rev-parse "${final_tag}" >/dev/null 2>&1; then
  print_error "Final release tag ${final_tag} already exists"
  exit 1
fi

step_summary "| Parameter | Value |"
step_summary "| --- | --- |"
step_summary "| Version | \`${version}\` |"
step_summary "| RC tag | \`${rc_tag}\` |"
step_summary "| Final tag | \`${final_tag}\` |"
step_summary "| Staging repo | \`${staging_repo_id}\` |"
step_summary "| Next dev version | \`${next_dev_version}-SNAPSHOT\` |"
step_summary "| Commit | \`${rc_commit}\` |"

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
# Step 1: Move SVN artifacts from dist/dev to dist/release
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### SVN Promotion"

svn_promote_rc_to_release "${version}" "${rc_num}"

step_summary "Promoted \`${rc_tag}\` -> \`${final_tag}\` on dist.apache.org"

# ---------------------------------------------------------------------------
# Step 2: Clean up old releases from dist/release
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Old Release Cleanup"

if [[ ${DRY_RUN:-1} -ne 1 ]]; then
  if ! old_versions=$(svn_list_old_releases "${version}"); then
    exit 1
  fi

  if [[ -n "${old_versions}" ]]; then
    step_summary "Removing old releases:"
    while IFS= read -r old_dir; do
      [[ -z "${old_dir}" ]] && continue
      svn_remove_release "${old_dir}" "${version}"
      step_summary "- Removed \`${old_dir}\`"
    done <<< "${old_versions}"
  else
    step_summary "No old releases to clean up"
  fi
else
  print_command "Dry-run, WOULD clean up old releases from dist/release"
  step_summary "Would clean up old releases (dry-run)"
fi

# ---------------------------------------------------------------------------
# Step 3: Create final release tag
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Release Tag"

exec_process git tag -a "${final_tag}" "${rc_commit}" -m "Release Apache Parquet ${version}"
exec_process git push origin "${final_tag}"

step_summary "Created tag \`${final_tag}\` at \`${rc_commit}\`"

# ---------------------------------------------------------------------------
# Step 4: Release Nexus staging repo
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Nexus Release"

nexus_release_staging_repo "${staging_repo_id}" "Apache Parquet ${version}"

step_summary "Released staging repository \`${staging_repo_id}\` to Maven Central"

# ---------------------------------------------------------------------------
# Step 5: Create GitHub Release
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### GitHub Release"

if [[ -n "${GITHUB_TOKEN:-}" ]]; then
  # If a pre-release exists for the RC tag, update it; otherwise create a new release
  if gh release view "${rc_tag}" &>/dev/null 2>&1; then
    print_info "Found existing pre-release for ${rc_tag}"
  fi

  exec_process gh release create "${final_tag}" \
    --title "Apache Parquet ${version}" \
    --generate-notes \
    --latest \
    --target "${rc_commit}"

  step_summary "Created GitHub release for \`${final_tag}\`"
else
  print_warning "GITHUB_TOKEN not set, skipping GitHub release creation"
  step_summary "Skipped GitHub release (no GITHUB_TOKEN)"
fi

# ---------------------------------------------------------------------------
# Step 6: Bump to next development version
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Version Bump"

next_snapshot="${next_dev_version}-SNAPSHOT"
print_info "Bumping version to ${next_snapshot}..."

set_pom_version "${next_snapshot}"

# Update previous.version property
exec_process ./mvnw -pl . versions:set-property \
  -Dproperty=previous.version -DnewVersion="${version}" \
  --batch-mode -q

exec_process git add -A
exec_process git commit -m "Prepare for next development iteration (${next_snapshot})"

# Refuse to push the version bump to anything that doesn't look like a
# release branch. Otherwise a workflow run dispatched against master
# (e.g. after `prepare-rc.sh --skip-branch-creation`) would push the
# next-dev-version commit straight to master.
current_branch=$(git rev-parse --abbrev-ref HEAD)
expected_branch="${BRANCH_PREFIX}${major}.${minor}.x"
if [[ "${current_branch}" != "${expected_branch}" ]]; then
  print_error "Refusing to push version bump: current branch is '${current_branch}'"
  print_error "Expected to be on release branch '${expected_branch}' for version ${version}."
  print_error "Run the publish workflow with that branch selected, or check out '${expected_branch}' locally."
  exit 1
fi
# Also confirm the format independently in case someone has named a
# non-release branch to look like one.
if ! validate_and_extract_branch_version "${current_branch}"; then
  print_error "Branch '${current_branch}' is not a valid release branch name."
  exit 1
fi
exec_process git push origin HEAD

step_summary "Bumped version to \`${next_snapshot}\` on branch \`${current_branch}\`, set \`previous.version=${version}\`"

# ---------------------------------------------------------------------------
# Step 7: Generate announce email
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Announce Email"
step_summary ""
step_summary '```'
step_summary "Subject: [ANNOUNCE] Apache Parquet ${version}"
step_summary ""
step_summary "I'm pleased to announce the release of Apache Parquet ${version}!"
step_summary ""
step_summary "Parquet is a general-purpose columnar file format for nested data. It uses"
step_summary "space-efficient encodings and a compressed and splittable structure for"
step_summary "processing frameworks like Hadoop."
step_summary ""
step_summary "Changes are listed at: https://github.com/apache/parquet-java/releases/tag/${final_tag}"
step_summary ""
step_summary "This release can be downloaded from: https://parquet.apache.org/downloads/"
step_summary ""
step_summary "Java artifacts are available from Maven Central."
step_summary ""
step_summary "Thanks to everyone for contributing!"
step_summary '```'

# ---------------------------------------------------------------------------
# Step 8: Reminder -- update parquet.apache.org
# ---------------------------------------------------------------------------
release_date=$(date +%Y-%m-%d)
step_summary ""
step_summary "### Manual Follow-up: Update parquet.apache.org"
step_summary ""
step_summary "Create a release blog post PR against \`apache/parquet-site\`."
step_summary "Add a new file \`content/en/blog/parquet-java/parquet-java-${version}.md\` with:"
step_summary ""
step_summary '```markdown'
step_summary "---"
step_summary "title: \"Apache Parquet Java ${version}\""
step_summary "date: ${release_date}"
step_summary "summary: \"Release notes for Apache Parquet Java ${version}\""
step_summary "---"
step_summary ""
step_summary "Apache Parquet Java ${version} has been released."
step_summary ""
step_summary "For the full list of changes, see the"
step_summary "[release notes](https://github.com/apache/parquet-java/releases/tag/${final_tag})."
step_summary ""
step_summary "Java artifacts are available from"
step_summary "[Maven Central](https://search.maven.org/search?q=g:org.apache.parquet%20AND%20v:${version})."
step_summary ""
step_summary "Source and binary downloads are available from the"
step_summary "[Apache downloads page](https://parquet.apache.org/downloads/)."
step_summary '```'
step_summary ""
step_summary "Submit the PR against the \`staging\` branch of"
step_summary "[\`apache/parquet-site\`](https://github.com/apache/parquet-site)."

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
step_summary ""
step_summary "---"
step_summary "### Summary"
step_summary ""
step_summary "| Step | Status |"
step_summary "| --- | --- |"
step_summary "| SVN promotion | done |"
step_summary "| Old release cleanup | done |"
step_summary "| Final release tag | \`${final_tag}\` |"
step_summary "| Nexus release | \`${staging_repo_id}\` released |"
step_summary "| GitHub release | created |"
step_summary "| Version bump | \`${next_snapshot}\` |"
step_summary "| Announce email | generated |"
step_summary "| Site update | **manual** -- see template above |"

print_success "Release ${version} published successfully!"
