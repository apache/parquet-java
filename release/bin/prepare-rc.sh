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
source "${LIBS_DIR}/_github.sh"
source "${LIBS_DIR}/_nexus.sh"
source "${LIBS_DIR}/_maven.sh"

trap 'rm -f .release-settings.xml' EXIT

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------
function usage {
  cat <<EOF
Usage: $0 <version> [OPTIONS]

Prepare a release candidate for Apache Parquet Java.

Arguments:
  version               Release version (e.g., 1.18.0)

Options:
  --rc <num>            Override RC number (default: auto-detect)
  --skip-branch-creation  Do not create the release branch
  --help                Show this help

Environment variables:
  DRY_RUN               Set to 0 for real execution (default: 1)
  NEXUS_USERNAME        Apache Nexus username
  NEXUS_PASSWORD        Apache Nexus password
  SVN_USERNAME          SVN username for dist.apache.org
  SVN_PASSWORD          SVN password
  GITHUB_TOKEN          GitHub token for CI checks and release creation

Example:
  DRY_RUN=1 $0 1.18.0
  DRY_RUN=0 $0 1.18.0 --rc 2
EOF
  exit "${1:-0}"
}

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
version=""
rc_override=""
skip_branch=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rc)
      if [[ -z "${2:-}" ]]; then
        print_error "--rc requires a value"
        usage 1
      fi
      rc_override="$2"
      if [[ ! "${rc_override}" =~ ^[0-9]+$ ]]; then
        print_error "--rc value must be a non-negative integer, got: '${rc_override}'"
        exit 1
      fi
      shift 2
      ;;
    --skip-branch-creation)
      skip_branch=true
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
      if [[ -z "${version}" ]]; then
        version="$1"
      else
        print_error "Unexpected argument: $1"
        usage 1
      fi
      shift
      ;;
  esac
done

if [[ -z "${version}" ]]; then
  print_error "Version is required"
  usage 1
fi

# ---------------------------------------------------------------------------
# Step 0: Validate inputs
# ---------------------------------------------------------------------------
step_summary "## Release Candidate Preparation"
step_summary ""

if [[ ${DRY_RUN:-1} -eq 1 ]]; then
  step_summary "> **DRY RUN** -- no changes will be made"
  step_summary ""
fi

if ! validate_and_extract_version "${version}"; then
  print_error "Invalid version format: '${version}'. Expected: X.Y.Z"
  exit 1
fi

step_summary "| Parameter | Value |"
step_summary "| --- | --- |"
step_summary "| Version | \`${version}\` |"

# Check prerequisites
if ! command -v gpg &>/dev/null; then
  print_warning "gpg not found -- GPG signing will fail"
fi
if ! command -v svn &>/dev/null; then
  print_warning "svn not found -- SVN staging will fail"
fi
if [[ ! -x ./mvnw ]]; then
  print_error "mvnw not found in current directory. Run from the repo root."
  exit 1
fi

# ---------------------------------------------------------------------------
# Step 1: Create release branch (idempotent)
# ---------------------------------------------------------------------------
release_branch="${BRANCH_PREFIX}${major}.${minor}.x"
step_summary "| Release branch | \`${release_branch}\` |"

if [[ "${skip_branch}" == "true" ]]; then
  print_info "Skipping branch creation (--skip-branch-creation)"
elif git show-ref --verify --quiet "refs/remotes/origin/${release_branch}" 2>/dev/null; then
  print_info "Release branch ${release_branch} already exists, skipping creation"
else
  print_info "Creating release branch ${release_branch} from master..."
  exec_process git branch "${release_branch}" origin/master
  exec_process git push origin "${release_branch}" --set-upstream
  step_summary ""
  step_summary "Created release branch \`${release_branch}\`"
fi

# Switch to the release branch
if [[ "$(git branch --show-current)" != "${release_branch}" ]]; then
  print_info "Switching to ${release_branch}..."
  exec_process git checkout "${release_branch}"
fi

# ---------------------------------------------------------------------------
# Step 2: Auto-detect RC number
# ---------------------------------------------------------------------------
if [[ -n "${rc_override}" ]]; then
  rc_number="${rc_override}"
  print_info "Using RC override: rc${rc_number}"
else
  find_next_rc_number "${version}"
  print_info "Auto-detected next RC: rc${rc_number}"
fi

rc_tag="${TAG_PREFIX}${version}-rc${rc_number}"
step_summary "| RC number | \`${rc_number}\` |"
step_summary "| RC tag | \`${rc_tag}\` |"

# Check if tag already exists
if git rev-parse "${rc_tag}" >/dev/null 2>&1; then
  print_error "Tag ${rc_tag} already exists. Use --rc to specify a different RC number."
  exit 1
fi

# ---------------------------------------------------------------------------
# Step 3: Verify CI checks
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### CI Verification"

current_commit=$(git rev-parse HEAD)
step_summary "| Commit | \`${current_commit}\` |"

if ! check_github_checks_passed "${current_commit}"; then
  print_error "CI checks are not passing. Fix CI before creating an RC."
  step_summary "CI checks: **FAILED**"
  exit 1
fi
step_summary "CI checks: **PASSED**"

# ---------------------------------------------------------------------------
# Step 4: Set POM versions (only on rc0 or if version doesn't match)
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Version Update"

current_pom_version=$(get_current_pom_version || echo "unknown")
print_info "Current POM version: ${current_pom_version}"

if [[ "${current_pom_version}" == "${version}" ]]; then
  print_info "POM version already set to ${version}, skipping version update"
  step_summary "POM version already at \`${version}\`, no update needed"
else
  print_info "Setting POM version to ${version}..."
  set_pom_version "${version}"
  step_summary "Updated POM version: \`${current_pom_version}\` -> \`${version}\`"

  # Commit version changes
  exec_process git add -A
  exec_process git commit -m "Set version to ${version} for release"
fi

# ---------------------------------------------------------------------------
# Step 5: Create RC tag and push
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Tag and Push"

exec_process git tag -a "${rc_tag}" -m "Apache Parquet ${version} RC${rc_number}"
exec_process git push origin "${release_branch}"
exec_process git push origin "${rc_tag}"

tag_commit=$(git rev-parse HEAD)
step_summary "Created tag \`${rc_tag}\` at \`${tag_commit}\`"

# ---------------------------------------------------------------------------
# Step 6: Deploy to Nexus
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Nexus Deployment"

settings_file=".release-settings.xml"
generate_maven_settings "${settings_file}"

maven_deploy "${settings_file}"

step_summary "Deployed artifacts to Apache Nexus staging"

# Find and close the staging repo
nexus_find_open_staging_repo "org.apache.parquet"
nexus_close_staging_repo "${staging_repo_id}" "Apache Parquet ${version} RC${rc_number}"

step_summary "Closed staging repository: \`${staging_repo_id}\`"

maven_cleanup_settings "${settings_file}"

# ---------------------------------------------------------------------------
# Step 7: Build source tarball
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Source Tarball"

tarball_name="${TAG_PREFIX}${version}.tar.gz"

if [[ ${DRY_RUN:-1} -ne 1 ]]; then
  release_hash=$(git rev-list -1 "${rc_tag}")
else
  release_hash=$(git rev-parse HEAD)
fi

print_info "Building source tarball from ${rc_tag} (${release_hash})..."

exec_process git archive "${release_hash}" --prefix "${TAG_PREFIX}${version}/" -o "${tarball_name}"
exec_process gpg --armor --output "${tarball_name}.asc" --detach-sig "${tarball_name}"
calculate_sha512 "${tarball_name}"

step_summary "Built \`${tarball_name}\` from \`${release_hash}\`"

# ---------------------------------------------------------------------------
# Step 8: Stage to SVN dist/dev
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### SVN Staging"

svn_dir="${APACHE_DIST_URL}${APACHE_DIST_DEV_PATH}"
rc_svn_dir="${rc_tag}"

if [[ ${DRY_RUN:-1} -ne 1 ]]; then
  if [[ -d tmp/ ]]; then
    rm -rf tmp/
  fi

  exec_process_with_retries 5 60 "tmp" \
    svn co --depth=empty --username "${SVN_USERNAME}" --password "${SVN_PASSWORD}" --non-interactive \
    "${svn_dir}" tmp

  mkdir -p "tmp/${rc_svn_dir}"
  cp "${tarball_name}" "${tarball_name}.asc" "${tarball_name}.sha512" "tmp/${rc_svn_dir}/"

  (cd tmp && exec_process svn add "${rc_svn_dir}")
  (cd tmp && exec_process svn ci \
    --username "${SVN_USERNAME}" --password "${SVN_PASSWORD}" --non-interactive \
    -m "Apache Parquet ${version} RC${rc_number}")

  rm -rf tmp
else
  print_command "Dry-run, WOULD stage to ${svn_dir}/${rc_svn_dir}"
fi

step_summary "Staged source tarball to \`${svn_dir}/${rc_svn_dir}\`"

# ---------------------------------------------------------------------------
# Step 9: Create GitHub pre-release
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### GitHub Pre-Release"

if [[ -n "${GITHUB_TOKEN:-}" ]]; then
  exec_process gh release create "${rc_tag}" \
    --title "Apache Parquet ${version} RC${rc_number}" \
    --prerelease \
    --generate-notes \
    --target "${tag_commit}"
  step_summary "Created GitHub pre-release for \`${rc_tag}\`"
else
  print_warning "GITHUB_TOKEN not set, skipping GitHub pre-release creation"
  step_summary "Skipped GitHub pre-release (no GITHUB_TOKEN)"
fi

# ---------------------------------------------------------------------------
# Step 10: Generate vote email
# ---------------------------------------------------------------------------
step_summary ""
step_summary "### Vote Email"
step_summary ""
step_summary '```'
step_summary "Subject: [VOTE] Release Apache Parquet ${version} RC${rc_number}"
step_summary ""
step_summary "Hi everyone,"
step_summary ""
step_summary "I propose the following RC to be released as official Apache Parquet ${version} release."
step_summary ""
step_summary "The commit id is ${tag_commit}"
step_summary "* This corresponds to the tag: ${rc_tag}"
step_summary "* https://github.com/apache/parquet-java/tree/${tag_commit}"
step_summary ""
step_summary "The release tarball, signature, and checksums are here:"
step_summary "* https://dist.apache.org/repos/dist/dev/parquet/${rc_tag}"
step_summary ""
step_summary "You can find the KEYS file here:"
step_summary "* https://downloads.apache.org/parquet/KEYS"
step_summary ""
step_summary "You can find the changelog here:"
step_summary "https://github.com/apache/parquet-java/releases/tag/${rc_tag}"
step_summary ""
step_summary "Binary artifacts are staged in Nexus here:"
step_summary "* ${NEXUS_STAGING_GROUP_URL}"
step_summary "* Staging repository ID: ${staging_repo_id:-UNKNOWN}"
step_summary ""
step_summary "Please download, verify, and test."
step_summary ""
step_summary "Please vote in the next 72 hours."
step_summary ""
step_summary "[ ] +1 Release this as Apache Parquet ${version}"
step_summary "[ ] +0"
step_summary "[ ] -1 Do not release this because..."
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
step_summary "| Release branch | \`${release_branch}\` |"
step_summary "| RC tag | \`${rc_tag}\` |"
step_summary "| Nexus staging repo | \`${staging_repo_id:-UNKNOWN}\` |"
step_summary "| Source tarball | \`${tarball_name}\` |"
step_summary "| SVN dist/dev | staged |"
step_summary "| GitHub pre-release | created |"
step_summary "| Vote email | generated |"

print_success "Release candidate ${rc_tag} prepared successfully!"
