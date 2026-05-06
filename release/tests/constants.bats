#!/usr/bin/env bats
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

setup() {
  load test_helper/common
  source "${LIBS_DIR}/_constants.sh"
}

@test "TAG_PREFIX is apache-parquet-" {
  [ "$TAG_PREFIX" = "apache-parquet-" ]
}

@test "BRANCH_PREFIX is parquet-" {
  [ "$BRANCH_PREFIX" = "parquet-" ]
}

@test "APACHE_DIST_URL has correct default" {
  [ "$APACHE_DIST_URL" = "https://dist.apache.org/repos/dist" ]
}

@test "APACHE_DIST_DEV_PATH is /dev/parquet" {
  [ "$APACHE_DIST_DEV_PATH" = "/dev/parquet" ]
}

@test "APACHE_DIST_RELEASE_PATH is /release/parquet" {
  [ "$APACHE_DIST_RELEASE_PATH" = "/release/parquet" ]
}

@test "NEXUS_BASE_URL has correct default" {
  [ "$NEXUS_BASE_URL" = "https://repository.apache.org/service/local" ]
}

@test "DRY_RUN defaults to 1" {
  [ "$DRY_RUN" = "1" ]
}

@test "VERSION_REGEX matches semver components" {
  [[ "1.18.0" =~ ^${VERSION_REGEX}$ ]]
  [ "${BASH_REMATCH[1]}" = "1" ]
  [ "${BASH_REMATCH[2]}" = "18" ]
  [ "${BASH_REMATCH[3]}" = "0" ]
}

@test "VERSION_REGEX_GIT_TAG matches RC tag" {
  [[ "apache-parquet-1.18.0-rc3" =~ ${VERSION_REGEX_GIT_TAG} ]]
  [ "${BASH_REMATCH[1]}" = "1" ]
  [ "${BASH_REMATCH[4]}" = "3" ]
}

@test "VERSION_REGEX_GIT_TAG rejects final tag" {
  [[ ! "apache-parquet-1.18.0" =~ ${VERSION_REGEX_GIT_TAG} ]]
}

@test "BRANCH_VERSION_REGEX matches parquet-1.18.x" {
  [[ "parquet-1.18.x" =~ ${BRANCH_VERSION_REGEX} ]]
  [ "${BASH_REMATCH[1]}" = "1" ]
  [ "${BASH_REMATCH[2]}" = "18" ]
}

@test "BRANCH_VERSION_REGEX rejects parquet-1.18.0" {
  [[ ! "parquet-1.18.0" =~ ${BRANCH_VERSION_REGEX} ]]
}

@test "GITHUB_REPO has correct default" {
  [ "$GITHUB_REPO" = "apache/parquet-java" ]
}
