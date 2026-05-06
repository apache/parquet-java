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

function nexus_find_open_staging_repo {
  local profile_name="${1:-org.apache.parquet}"

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
