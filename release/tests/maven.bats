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
  MAVEN_DEPLOY_LOG="${TEST_TMPDIR}/target/nexus-deploy.log"
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
  run maven_deploy "${settings_file}" "Apache Parquet 1.2.3 RC4"
  [ "$status" -eq 0 ]
  [[ "$output" == *"Dry-run"* ]]
  [[ "$output" == *"deploy"* ]]
}

@test "maven_deploy: includes apache-release profile in dry-run output" {
  DRY_RUN=1
  local settings_file="${TEST_TMPDIR}/settings.xml"
  generate_maven_settings "${settings_file}"
  run maven_deploy "${settings_file}" "Apache Parquet 1.2.3 RC4"
  [[ "$output" == *"apache-release"* ]]
}

@test "maven_deploy: includes skipTests in dry-run output" {
  DRY_RUN=1
  local settings_file="${TEST_TMPDIR}/settings.xml"
  generate_maven_settings "${settings_file}"
  run maven_deploy "${settings_file}" "Apache Parquet 1.2.3 RC4"
  [[ "$output" == *"skipTests"* ]]
}

@test "maven_deploy: passes staging description in dry-run output" {
  DRY_RUN=1
  local settings_file="${TEST_TMPDIR}/settings.xml"
  generate_maven_settings "${settings_file}"
  run maven_deploy "${settings_file}" "Apache Parquet 1.2.3 RC4"
  [[ "$output" == *"-DstagingDescription=Apache Parquet 1.2.3 RC4"* ]]
}

@test "maven_deploy: clears local staging directory in dry-run output" {
  DRY_RUN=1
  local settings_file="${TEST_TMPDIR}/settings.xml"
  generate_maven_settings "${settings_file}"
  run maven_deploy "${settings_file}" "Apache Parquet 1.2.3 RC4"
  [[ "$output" == *"rm -rf target/nexus-staging"* ]]
}

@test "maven_deploy: real mode passes description as one argument, logs output, and propagates failure" {
  DRY_RUN=0
  cd "${TEST_TMPDIR}"
  mkdir -p target/nexus-staging/staging
  touch target/nexus-staging/staging/stale.properties
  cat > mvnw <<'EOF'
#!/bin/bash
printf '%s\n' "$@" > mvnw-args.txt
echo ' * Created staging repository with ID "orgapacheparquet-1091".'
exit 3
EOF
  chmod +x mvnw
  local settings_file="${TEST_TMPDIR}/settings.xml"
  generate_maven_settings "${settings_file}"
  run maven_deploy "${settings_file}" "Apache Parquet 1.2.3 RC4"
  [ "$status" -eq 3 ]
  [ ! -e target/nexus-staging ]
  grep -qx -- "-DstagingDescription=Apache Parquet 1.2.3 RC4" mvnw-args.txt
  grep -q 'orgapacheparquet-1091' "${MAVEN_DEPLOY_LOG}"
}

# ---- maven_read_staging_repo_id ----

@test "maven_read_staging_repo_id: dry-run returns placeholder" {
  DRY_RUN=1
  maven_read_staging_repo_id "${TEST_TMPDIR}"
  [ "${staging_repo_id}" = "DRY-RUN-REPO-ID" ]
}

@test "maven_read_staging_repo_id: reads ID from plugin properties file" {
  DRY_RUN=0
  mkdir -p "${TEST_TMPDIR}/staging"
  cat > "${TEST_TMPDIR}/staging/1ab2c3d4e5f6.properties" <<'EOF'
#Generated by org.sonatype.plugins:nexus-staging-maven-plugin:1.7.0
stagingRepository.managed=true
stagingRepository.id=orgapacheparquet-1090
stagingRepository.profileId=1ab2c3d4e5f6
stagingRepository.url=https\://repository.apache.org/content/repositories/orgapacheparquet-1090
EOF
  maven_read_staging_repo_id "${TEST_TMPDIR}"
  [ "${staging_repo_id}" = "orgapacheparquet-1090" ]
}

@test "maven_read_staging_repo_id: fails when no properties file exists" {
  DRY_RUN=0
  mkdir -p "${TEST_TMPDIR}/staging"
  run maven_read_staging_repo_id "${TEST_TMPDIR}"
  [ "$status" -eq 1 ]
  [[ "$output" == *"Expected one staging properties file"* ]]
}

@test "maven_read_staging_repo_id: fails when properties file has no ID" {
  DRY_RUN=0
  mkdir -p "${TEST_TMPDIR}/staging"
  echo "stagingRepository.profileId=1ab2c3d4e5f6" > "${TEST_TMPDIR}/staging/1ab2c3d4e5f6.properties"
  run maven_read_staging_repo_id "${TEST_TMPDIR}"
  [ "$status" -eq 1 ]
  [[ "$output" == *"No stagingRepository.id"* ]]
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

@test "maven_read_staging_repo_id: fails when more than one properties file exists" {
  DRY_RUN=0
  mkdir -p "${TEST_TMPDIR}/staging"
  echo "stagingRepository.id=orgapacheparquet-1" > "${TEST_TMPDIR}/staging/a.properties"
  echo "stagingRepository.id=orgapacheparquet-2" > "${TEST_TMPDIR}/staging/b.properties"
  run maven_read_staging_repo_id "${TEST_TMPDIR}"
  [ "$status" -eq 1 ]
  [[ "$output" == *"a.properties"* ]]
  [[ "$output" == *"b.properties"* ]]
}

@test "maven_read_staging_repo_id: defaults to the plugin's staging directory" {
  DRY_RUN=0
  cd "${TEST_TMPDIR}"
  mkdir -p target/nexus-staging/staging
  echo "stagingRepository.id=orgapacheparquet-1092" > target/nexus-staging/staging/x.properties
  maven_read_staging_repo_id
  [ "${staging_repo_id}" = "orgapacheparquet-1092" ]
}

# ---- maven_find_created_staging_repos ----

@test "maven_find_created_staging_repos: finds repository IDs in the deploy log" {
  mkdir -p "$(dirname "${MAVEN_DEPLOY_LOG}")"
  cat > "${MAVEN_DEPLOY_LOG}" <<'EOF'
[INFO]  * Created staging repository with ID "orgapacheparquet-1093".
[ERROR] Rule failure while trying to close staging repository with ID "orgapacheparquet-1093".
[ERROR]  * Not dropping failed staging repository with ID "orgapacheparquet-1093" (Rule failure).
EOF
  maven_find_created_staging_repos
  [ "${#created_staging_repo_ids[@]}" -eq 1 ]
  [ "${created_staging_repo_ids[0]}" = "orgapacheparquet-1093" ]
}

@test "maven_find_created_staging_repos: fails when no repository was created" {
  mkdir -p "$(dirname "${MAVEN_DEPLOY_LOG}")"
  echo "[ERROR] COMPILATION ERROR" > "${MAVEN_DEPLOY_LOG}"
  run maven_find_created_staging_repos
  [ "$status" -eq 1 ]
}

@test "maven_find_created_staging_repos: fails when the deploy log is missing" {
  run maven_find_created_staging_repos "${TEST_TMPDIR}/missing.log"
  [ "$status" -eq 1 ]
}
