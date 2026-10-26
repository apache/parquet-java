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
  source "${LIBS_DIR}/_maven.sh"
  export NEXUS_USERNAME="testuser"
  export NEXUS_PASSWORD="testpass"
  TEST_TMPDIR=$(mktemp -d)
}

teardown() {
  rm -rf "${TEST_TMPDIR}"
}

# ---- generate_maven_settings ----

@test "generate_maven_settings: creates settings file" {
  local settings_file="${TEST_TMPDIR}/settings.xml"
  generate_maven_settings "${settings_file}"
  [ -f "${settings_file}" ]
}

@test "generate_maven_settings: includes server ID" {
  local settings_file="${TEST_TMPDIR}/settings.xml"
  generate_maven_settings "${settings_file}"
  [[ "$(cat "${settings_file}")" == *"apache.releases.https"* ]]
}

@test "generate_maven_settings: uses env var references instead of real credentials" {
  local settings_file="${TEST_TMPDIR}/settings.xml"
  generate_maven_settings "${settings_file}"
  local content
  content=$(cat "${settings_file}")
  [[ "$content" == *'${env.NEXUS_USERNAME}'* ]]
  [[ "$content" == *'${env.NEXUS_PASSWORD}'* ]]
  [[ "$content" != *"testuser"* ]]
  [[ "$content" != *"testpass"* ]]
}

@test "generate_maven_settings: enables GPG agent" {
  local settings_file="${TEST_TMPDIR}/settings.xml"
  generate_maven_settings "${settings_file}"
  [[ "$(cat "${settings_file}")" == *"gpg.useagent"* ]]
  [[ "$(cat "${settings_file}")" == *"true"* ]]
}

@test "generate_maven_settings: produces valid XML" {
  local settings_file="${TEST_TMPDIR}/settings.xml"
  generate_maven_settings "${settings_file}"
  # Check that xmllint can parse it, if available
  if command -v xmllint &>/dev/null; then
    run xmllint --noout "${settings_file}"
    [ "$status" -eq 0 ]
  else
    # Fallback: check basic XML structure
    [[ "$(head -1 "${settings_file}")" == *"<?xml"* ]]
    [[ "$(cat "${settings_file}")" == *"</settings>"* ]]
  fi
}

# ---- maven_deploy ----

@test "maven_deploy: dry-run does not execute mvnw" {
  DRY_RUN=1
  local settings_file="${TEST_TMPDIR}/settings.xml"
  generate_maven_settings "${settings_file}"
  run maven_deploy "${settings_file}"
  [ "$status" -eq 0 ]
  [[ "$output" == *"Dry-run"* ]]
  [[ "$output" == *"deploy"* ]]
}

@test "maven_deploy: includes apache-release profile in dry-run output" {
  DRY_RUN=1
  local settings_file="${TEST_TMPDIR}/settings.xml"
  generate_maven_settings "${settings_file}"
  run maven_deploy "${settings_file}"
  [[ "$output" == *"apache-release"* ]]
}

@test "maven_deploy: includes skipTests in dry-run output" {
  DRY_RUN=1
  local settings_file="${TEST_TMPDIR}/settings.xml"
  generate_maven_settings "${settings_file}"
  run maven_deploy "${settings_file}"
  [[ "$output" == *"skipTests"* ]]
}

# ---- maven_cleanup_settings ----

@test "maven_cleanup_settings: removes settings file" {
  local settings_file="${TEST_TMPDIR}/settings.xml"
  echo "test" > "${settings_file}"
  maven_cleanup_settings "${settings_file}"
  [ ! -f "${settings_file}" ]
}

@test "maven_cleanup_settings: no error if file missing" {
  run maven_cleanup_settings "${TEST_TMPDIR}/nonexistent.xml"
  [ "$status" -eq 0 ]
}
