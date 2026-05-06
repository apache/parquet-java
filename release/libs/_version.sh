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

[[ -n "${_VERSION_LOADED:-}" ]] && return 0 2>/dev/null || true
_VERSION_LOADED=1

LIBS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "$LIBS_DIR/_constants.sh"
source "$LIBS_DIR/_exec.sh"

function validate_and_extract_version {
  local version="$1"
  if [[ ! ${version} =~ ^${VERSION_REGEX}$ ]]; then
    return 1
  fi
  major="${BASH_REMATCH[1]}"
  minor="${BASH_REMATCH[2]}"
  patch="${BASH_REMATCH[3]}"
  version_without_rc="${major}.${minor}.${patch}"
  return 0
}

function validate_and_extract_git_tag_version {
  local tag="$1"
  if [[ ! ${tag} =~ ${VERSION_REGEX_GIT_TAG} ]]; then
    return 1
  fi
  major="${BASH_REMATCH[1]}"
  minor="${BASH_REMATCH[2]}"
  patch="${BASH_REMATCH[3]}"
  rc_number="${BASH_REMATCH[4]}"
  version_without_rc="${major}.${minor}.${patch}"
  return 0
}

function validate_and_extract_branch_version {
  local branch="$1"
  if [[ ! ${branch} =~ ${BRANCH_VERSION_REGEX} ]]; then
    return 1
  fi
  major="${BASH_REMATCH[1]}"
  minor="${BASH_REMATCH[2]}"
  return 0
}

function find_next_rc_number {
  local version_without_rc="$1"
  local tag_pattern="${TAG_PREFIX}${version_without_rc}-rc*"
  local existing_tags
  existing_tags=$(git tag -l "${tag_pattern}" | sort -V)

  if [[ -z "${existing_tags}" ]]; then
    rc_number=0
  else
    local highest_rc
    highest_rc=$(echo "${existing_tags}" | sed "s/${TAG_PREFIX}${version_without_rc}-rc//" | sort -n | tail -1)
    rc_number=$((highest_rc + 1))
  fi
  return 0
}

function find_latest_rc_number {
  local version_without_rc="$1"
  local tag_pattern="${TAG_PREFIX}${version_without_rc}-rc*"
  local existing_tags
  existing_tags=$(git tag -l "${tag_pattern}" | sort -V)

  if [[ -z "${existing_tags}" ]]; then
    print_error "No RC tags found for version ${version_without_rc}"
    return 1
  fi

  latest_rc_number=$(echo "${existing_tags}" | sed "s/${TAG_PREFIX}${version_without_rc}-rc//" | sort -n | tail -1)
  return 0
}

function find_next_patch_number {
  local major="$1"
  local minor="$2"
  local rc_tag_pattern="${TAG_PREFIX}${major}.${minor}.*-rc*"
  local existing_rc_tags
  existing_rc_tags=$(git tag -l "${rc_tag_pattern}" | sort -V)

  if [[ -z "${existing_rc_tags}" ]]; then
    patch=0
  else
    local highest_patch=-1
    while IFS= read -r tag; do
      if [[ ${tag} =~ ${TAG_PREFIX}${major}\.${minor}\.([0-9]+)-rc[0-9]+ ]]; then
        local current_patch="${BASH_REMATCH[1]}"
        if [[ ${current_patch} -gt ${highest_patch} ]]; then
          highest_patch=${current_patch}
        fi
      fi
    done <<< "${existing_rc_tags}"

    local final_tag="${TAG_PREFIX}${major}.${minor}.${highest_patch}"
    if git rev-parse "${final_tag}" >/dev/null 2>&1; then
      patch=$((highest_patch + 1))
    else
      patch=${highest_patch}
    fi
  fi
  return 0
}

function set_pom_version {
  local version="$1"
  exec_process ./mvnw versions:set -DnewVersion="${version}" -DgenerateBackupPoms=false --batch-mode -q
}

function get_current_pom_version {
  ./mvnw help:evaluate -Dexpression=project.version -q -DforceStdout 2>/dev/null
}
