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

# Helpers for talking to dist.apache.org via svn.
#
# Required env vars: SVN_USERNAME, SVN_PASSWORD.
# All operations honor DRY_RUN through exec_process / explicit guards.

[[ -n "${_SVN_LOADED:-}" ]] && return 0 2>/dev/null || true
_SVN_LOADED=1

LIBS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${LIBS_DIR}/_constants.sh"
source "${LIBS_DIR}/_log.sh"
source "${LIBS_DIR}/_exec.sh"

# Allow ${SVN_USERNAME}/${SVN_PASSWORD} to be expanded under `set -u`
# even before the operator has provided real credentials -- their
# absence is fine in dry-run; in real mode svn itself will reject the
# request.
: "${SVN_USERNAME:=}"
: "${SVN_PASSWORD:=}"

# svn_stage_rc <version> <rc_num> <files...>
#   Stages the given files under dist/dev/parquet/<rc_tag>/ via svn.
#   Uses a per-call mktemp checkout dir; cleans it up on success.
function svn_stage_rc {
  local version="$1"
  local rc_num="$2"
  shift 2
  local files=("$@")

  local rc_tag="${TAG_PREFIX}${version}-rc${rc_num}"
  local dev_url="${APACHE_DIST_URL}${APACHE_DIST_DEV_PATH}"
  local commit_msg="Apache Parquet ${version} RC${rc_num}"

  if [[ ${#files[@]} -eq 0 ]]; then
    print_error "svn_stage_rc: no files to stage"
    return 1
  fi

  if [[ ${DRY_RUN:-1} -eq 1 ]]; then
    print_command "Dry-run, WOULD stage to ${dev_url}/${rc_tag}: ${files[*]}"
    return 0
  fi

  local checkout_dir
  checkout_dir=$(mktemp -d -t parquet-release-svn-XXXXXX)
  # shellcheck disable=SC2064  # we want $checkout_dir expanded now, not at trap time
  trap "rm -rf '${checkout_dir}'" RETURN

  exec_process_with_retries 5 60 "${checkout_dir}" \
    svn co --depth=empty \
    --username "${SVN_USERNAME}" --password "${SVN_PASSWORD}" --non-interactive \
    "${dev_url}" "${checkout_dir}"

  mkdir -p "${checkout_dir}/${rc_tag}"
  cp "${files[@]}" "${checkout_dir}/${rc_tag}/"

  ( cd "${checkout_dir}" && exec_process svn add "${rc_tag}" )
  ( cd "${checkout_dir}" && exec_process svn ci \
      --username "${SVN_USERNAME}" --password "${SVN_PASSWORD}" --non-interactive \
      -m "${commit_msg}" )
}

# svn_promote_rc_to_release <version> <rc_num>
#   Moves dist/dev/parquet/<rc_tag>/ to dist/release/parquet/<final_tag>/.
function svn_promote_rc_to_release {
  local version="$1"
  local rc_num="$2"

  local rc_tag="${TAG_PREFIX}${version}-rc${rc_num}"
  local final_tag="${TAG_PREFIX}${version}"
  local dev_url="${APACHE_DIST_URL}${APACHE_DIST_DEV_PATH}/${rc_tag}"
  local release_url="${APACHE_DIST_URL}${APACHE_DIST_RELEASE_PATH}/${final_tag}"

  exec_process svn mv \
    --username "${SVN_USERNAME}" --password "${SVN_PASSWORD}" --non-interactive \
    "${dev_url}" "${release_url}" \
    -m "Release Apache Parquet ${version}"
}

# svn_remove_rc <version> <rc_num>
#   Removes dist/dev/parquet/<rc_tag>/ (used by cancel-rc.sh).
function svn_remove_rc {
  local version="$1"
  local rc_num="$2"

  local rc_tag="${TAG_PREFIX}${version}-rc${rc_num}"
  local dev_url="${APACHE_DIST_URL}${APACHE_DIST_DEV_PATH}/${rc_tag}"

  if [[ ${DRY_RUN:-1} -ne 1 ]]; then
    if ! svn ls --username "${SVN_USERNAME}" --password "${SVN_PASSWORD}" \
        --non-interactive "${dev_url}" >/dev/null 2>&1; then
      print_warning "SVN directory not found: ${dev_url} (already deleted?)"
      return 0
    fi
  fi

  exec_process svn rm \
    --username "${SVN_USERNAME}" --password "${SVN_PASSWORD}" --non-interactive \
    "${dev_url}" \
    -m "Cancel Apache Parquet ${version} RC${rc_num}"
}

# svn_list_old_releases <version_to_keep>
#   Echoes (one per line, on stdout) the names of release directories
#   under dist/release/parquet/ that should be removed when promoting
#   <version_to_keep> as the new latest release. Returns non-zero only
#   on real svn-listing failure.
function svn_list_old_releases {
  local version_to_keep="$1"
  local release_base_url="${APACHE_DIST_URL}${APACHE_DIST_RELEASE_PATH}"

  if [[ ${DRY_RUN:-1} -eq 1 ]]; then
    print_command "Dry-run, WOULD list ${release_base_url}" >&2
    return 0
  fi

  local listing
  if ! listing=$(svn list \
      --username "${SVN_USERNAME}" --password "${SVN_PASSWORD}" --non-interactive \
      "${release_base_url}" 2>&1); then
    print_error "Failed to list SVN releases at ${release_base_url}: ${listing}"
    return 1
  fi

  echo "${listing}" | grep -E "^${TAG_PREFIX}[0-9]" | sed 's|/$||' \
    | grep -v "^${TAG_PREFIX}${version_to_keep}\$" || true
}

# svn_remove_release <release_dir> <new_version>
#   svn rm a release directory under dist/release/parquet/.
function svn_remove_release {
  local release_dir="$1"
  local new_version="$2"
  local release_base_url="${APACHE_DIST_URL}${APACHE_DIST_RELEASE_PATH}"

  exec_process svn rm \
    --username "${SVN_USERNAME}" --password "${SVN_PASSWORD}" --non-interactive \
    "${release_base_url}/${release_dir}" \
    -m "Remove old release ${release_dir} (superseded by ${new_version})"
}
