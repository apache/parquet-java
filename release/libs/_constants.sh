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

[[ -n "${_CONSTANTS_LOADED:-}" ]] && return 0 2>/dev/null || true
_CONSTANTS_LOADED=1

TAG_PREFIX="apache-parquet-"
BRANCH_PREFIX="parquet-"

APACHE_DIST_URL=${APACHE_DIST_URL:-"https://dist.apache.org/repos/dist"}
APACHE_DIST_DEV_PATH="/dev/parquet"
APACHE_DIST_RELEASE_PATH="/release/parquet"

NEXUS_BASE_URL=${NEXUS_BASE_URL:-"https://repository.apache.org/service/local"}
NEXUS_STAGING_GROUP_URL="https://repository.apache.org/content/groups/staging/org/apache/parquet/"

DRY_RUN=${DRY_RUN:-1}

VERSION_REGEX="([0-9]+)\.([0-9]+)\.([0-9]+)"
VERSION_REGEX_GIT_TAG="^${TAG_PREFIX}${VERSION_REGEX}-rc([0-9]+)$"
VERSION_REGEX_FINAL_TAG="^${TAG_PREFIX}${VERSION_REGEX}$"
BRANCH_VERSION_REGEX="^parquet-([0-9]+)\.([0-9]+)\.x$"

GITHUB_REPO=${GITHUB_REPOSITORY:-"apache/parquet-java"}
