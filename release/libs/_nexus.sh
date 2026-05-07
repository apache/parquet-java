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

[[ -n "${_NEXUS_LOADED:-}" ]] && return 0 2>/dev/null || true
_NEXUS_LOADED=1

LIBS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${LIBS_DIR}/_constants.sh"
source "${LIBS_DIR}/_exec.sh"

function _nexus_bulk_action {
  local action="$1"
  local repo_id="$2"
  local description="$3"

  local url="${NEXUS_BASE_URL}/staging/bulk/${action}"
  local payload
  payload=$(jq -n --arg id "${repo_id}" --arg desc "${description}" \
    '{"data": {"stagedRepositoryIds": [$id], "description": $desc}}')

  print_info "Nexus ${action}: repo_id=${repo_id}"

  if [[ ${DRY_RUN:-1} -ne 1 ]]; then
    print_command "Executing 'curl --fail -X POST ${url}' (credentials via stdin)"
    curl --fail --silent --show-error \
      -K <(printf 'user = "%s:%s"\n' "${NEXUS_USERNAME}" "${NEXUS_PASSWORD}") \
      -H "Content-Type: application/json" \
      -d "${payload}" \
      "${url}"
  else
    print_command "Dry-run, WOULD POST to ${url} with payload for repo ${repo_id}"
  fi
}

function nexus_close_staging_repo {
  local repo_id="$1"
  local description="${2:-Closing staging repository}"
  _nexus_bulk_action "close" "${repo_id}" "${description}"
}

function nexus_release_staging_repo {
  local repo_id="$1"
  local description="${2:-Releasing staging repository}"
  _nexus_bulk_action "promote" "${repo_id}" "${description}"
}

function nexus_drop_staging_repo {
  local repo_id="$1"
  local description="${2:-Dropping staging repository}"
  _nexus_bulk_action "drop" "${repo_id}" "${description}"
}

function nexus_get_staging_repo_metadata {
  local repo_id="$1"
  local url="${NEXUS_BASE_URL}/staging/repository/${repo_id}"

  nexus_repo_profile=""
  nexus_repo_state=""
  nexus_repo_description=""

  if [[ ${DRY_RUN:-1} -eq 1 ]]; then
    print_command "Dry-run, WOULD GET ${url} (skipping metadata fetch)"
    nexus_repo_profile="${NEXUS_PROFILE_NAME}"
    nexus_repo_state="closed"
    nexus_repo_description="DRY_RUN_DESCRIPTION"
    return 0
  fi

  local response
  if ! response=$(curl --fail --silent --show-error \
    -K <(printf 'user = "%s:%s"\n' "${NEXUS_USERNAME}" "${NEXUS_PASSWORD}") \
    -H "Accept: application/json" \
    "${url}"); then
    print_error "Failed to fetch staging repository metadata for ${repo_id}"
    return 1
  fi

  nexus_repo_profile=$(echo "${response}" | jq -r '.profileName // ""')
  nexus_repo_state=$(echo "${response}" | jq -r '.type // ""')
  nexus_repo_description=$(echo "${response}" | jq -r '.description // ""')

  if [[ -z "${nexus_repo_profile}" || -z "${nexus_repo_state}" ]]; then
    print_error "Unable to parse staging repository metadata for ${repo_id}"
    return 1
  fi

  return 0
}

function nexus_check_staging_artifact {
  local repo_id="$1"
  local version="$2"
  local artifact_url="${NEXUS_CONTENT_BASE_URL}/${repo_id}/${NEXUS_VERIFY_GROUP_PATH}/${NEXUS_VERIFY_ARTIFACT_ID}/${version}/${NEXUS_VERIFY_ARTIFACT_ID}-${version}.pom"

  if [[ ${DRY_RUN:-1} -eq 1 ]]; then
    print_command "Dry-run, WOULD HEAD ${artifact_url}"
    return 0
  fi

  if ! curl --fail --silent --show-error --head \
    -K <(printf 'user = "%s:%s"\n' "${NEXUS_USERNAME}" "${NEXUS_PASSWORD}") \
    "${artifact_url}" >/dev/null; then
    print_error "Expected artifact not found in staging repo: ${artifact_url}"
    return 1
  fi

  return 0
}

function nexus_verify_staging_repo {
  local repo_id="$1"
  local version="$2"
  local rc_num="$3"
  local allow_description_mismatch="${4:-0}"

  local expected_description="Apache Parquet ${version} RC${rc_num}"

  print_info "Verifying staging repository ${repo_id}..."

  if ! nexus_get_staging_repo_metadata "${repo_id}"; then
    return 1
  fi

  if [[ "${nexus_repo_profile}" != "${NEXUS_PROFILE_NAME}" ]]; then
    print_error "Profile mismatch: expected '${NEXUS_PROFILE_NAME}', got '${nexus_repo_profile}'"
    print_error "This staging repo does not belong to Apache Parquet."
    return 1
  fi

  if [[ "${nexus_repo_state}" != "closed" ]]; then
    print_error "Unexpected state: expected 'closed', got '${nexus_repo_state}'"
    print_error "Staging repo must be closed (not open/released/dropped) before this action."
    return 1
  fi

  if ! nexus_check_staging_artifact "${repo_id}" "${version}"; then
    print_error "Verification failed: ${NEXUS_VERIFY_ARTIFACT_ID}-${version}.pom not found in staging repo."
    print_error "This staging repo does not appear to contain ${version} artifacts."
    return 1
  fi

  if [[ ${DRY_RUN:-1} -ne 1 && "${nexus_repo_description}" != *"${expected_description}"* ]]; then
    print_warning "Description mismatch: expected to contain '${expected_description}'"
    print_warning "Actual description: '${nexus_repo_description}'"
    if [[ "${allow_description_mismatch}" != "1" ]]; then
      print_error "Refusing to proceed. Re-run with --allow-description-mismatch to bypass."
      return 1
    fi
    print_warning "Continuing despite description mismatch (--allow-description-mismatch)."
  fi

  print_info "Staging repository ${repo_id} verified."
  return 0
}

function nexus_find_open_staging_repo {
  local profile_name="${1:-${NEXUS_PROFILE_NAME}}"

  print_info "Searching for open staging repository for ${profile_name}..."

  if [[ ${DRY_RUN:-1} -eq 1 ]]; then
    print_command "Dry-run, WOULD search Nexus for open staging repo"
    staging_repo_id="DRY-RUN-REPO-ID"
    return 0
  fi

  local response
  if ! response=$(curl --fail --silent --show-error \
    -K <(printf 'user = "%s:%s"\n' "${NEXUS_USERNAME}" "${NEXUS_PASSWORD}") \
    "${NEXUS_BASE_URL}/staging/profile_repositories"); then
    print_error "Failed to query Nexus staging repositories"
    return 1
  fi

  staging_repo_id=$(echo "${response}" | \
    NEXUS_PROFILE_NAME="${profile_name}" python3 -c "
import sys, os, xml.etree.ElementTree as ET
profile = os.environ['NEXUS_PROFILE_NAME']
tree = ET.parse(sys.stdin)
for repo in tree.findall('.//stagingProfileRepository'):
    repo_type = repo.find('type')
    repo_id = repo.find('repositoryId')
    if repo_type is not None and repo_type.text == 'open' and repo_id is not None:
        if profile.replace('.', '') in (repo_id.text or ''):
            print(repo_id.text)
            break
" 2>/dev/null)

  if [[ -z "${staging_repo_id}" ]]; then
    print_error "No open staging repository found for ${profile_name}"
    return 1
  fi

  print_info "Found staging repository: ${staging_repo_id}"
  return 0
}
