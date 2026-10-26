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

function validate_and_extract_branch_version {
  local branch="$1"
  if [[ ! ${branch} =~ ${BRANCH_VERSION_REGEX} ]]; then
    return 1
  fi
  major="${BASH_REMATCH[1]}"
  minor="${BASH_REMATCH[2]}"
  return 0
}

function _filter_rc_tags {
  local version_without_rc="$1"
  local exact_pattern="^${TAG_PREFIX}${version_without_rc}-rc[0-9]+$"
  git tag -l "${TAG_PREFIX}${version_without_rc}-rc*" | grep -E "${exact_pattern}"
}

function find_next_rc_number {
  local version_without_rc="$1"
  local existing_tags
  existing_tags=$(_filter_rc_tags "${version_without_rc}" || true)

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
  local existing_tags
  existing_tags=$(_filter_rc_tags "${version_without_rc}" || true)

  if [[ -z "${existing_tags}" ]]; then
    print_error "No RC tags found for version ${version_without_rc}"
    return 1
  fi

  latest_rc_number=$(echo "${existing_tags}" | sed "s/${TAG_PREFIX}${version_without_rc}-rc//" | sort -n | tail -1)
  return 0
}

function set_pom_version {
  local version="$1"
  exec_process ./mvnw versions:set -DnewVersion="${version}" -DgenerateBackupPoms=false --batch-mode -q
}

function get_current_pom_version {
  ./mvnw help:evaluate -Dexpression=project.version -q -DforceStdout 2>/dev/null
}
