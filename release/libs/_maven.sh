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

[[ -n "${_MAVEN_LOADED:-}" ]] && return 0 2>/dev/null || true
_MAVEN_LOADED=1

LIBS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${LIBS_DIR}/_constants.sh"
source "${LIBS_DIR}/_exec.sh"

function generate_maven_settings {
  local settings_file="${1:-.release-settings.xml}"

  if [[ -z "${NEXUS_USERNAME:-}" || -z "${NEXUS_PASSWORD:-}" ]]; then
    print_warning "NEXUS_USERNAME or NEXUS_PASSWORD not set; Maven deploy may fail"
  fi

  cat > "${settings_file}" <<'SETTINGS_EOF'
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.2.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.2.0 https://maven.apache.org/xsd/settings-1.2.0.xsd">
  <servers>
    <server>
      <id>apache.releases.https</id>
      <username>${env.NEXUS_USERNAME}</username>
      <password>${env.NEXUS_PASSWORD}</password>
    </server>
  </servers>
  <profiles>
    <profile>
      <id>gpg-release</id>
      <properties>
        <gpg.useagent>true</gpg.useagent>
      </properties>
    </profile>
  </profiles>
  <activeProfiles>
    <activeProfile>gpg-release</activeProfile>
  </activeProfiles>
</settings>
SETTINGS_EOF

  print_info "Generated Maven settings at ${settings_file} (credentials resolved from env vars at build time)"
}

# maven_deploy <settings-file> <staging-description>
#   Deploys through nexus-staging-maven-plugin, which opens one staging
#   repository, uploads every module into it, and closes it. Output is
#   also written to MAVEN_DEPLOY_LOG.
function maven_deploy {
  local settings_file="${1:-.release-settings.xml}"
  local staging_description="$2"

  if [[ ! -f "${settings_file}" ]]; then
    print_info "Generating Maven settings..."
    generate_maven_settings "${settings_file}"
  fi

  exec_process rm -rf "${NEXUS_LOCAL_STAGING_DIR}"
  mkdir -p "$(dirname "${MAVEN_DEPLOY_LOG}")"

  exec_process ./mvnw deploy \
    -Papache-release \
    -DskipTests \
    -Darguments=-DskipTests \
    "-DstagingDescription=${staging_description}" \
    --settings "${settings_file}" \
    --batch-mode | tee "${MAVEN_DEPLOY_LOG}"
  return "${PIPESTATUS[0]}"
}

# maven_read_staging_repo_id [local-staging-dir]
#   Sets staging_repo_id from the properties file nexus-staging-maven-plugin
#   writes after uploading, before it attempts the close.
function maven_read_staging_repo_id {
  local staging_dir="${1:-${NEXUS_LOCAL_STAGING_DIR}}/staging"

  if [[ ${DRY_RUN:-1} -eq 1 ]]; then
    print_command "Dry-run, WOULD read staging repository ID from ${staging_dir}/*.properties"
    staging_repo_id="DRY-RUN-REPO-ID"
    return 0
  fi

  local properties_files=("${staging_dir}"/*.properties)
  if [[ ${#properties_files[@]} -ne 1 || ! -f "${properties_files[0]}" ]]; then
    print_error "Expected one staging properties file in ${staging_dir}, found: ${properties_files[*]}"
    return 1
  fi

  staging_repo_id=$(sed -n 's/^stagingRepository\.id=//p' "${properties_files[0]}")
  if [[ -z "${staging_repo_id}" ]]; then
    print_error "No stagingRepository.id in ${properties_files[0]}"
    return 1
  fi

  print_info "Staging repository: ${staging_repo_id}"
  return 0
}

# maven_find_created_staging_repos [deploy-log]
#   Sets created_staging_repo_ids to every staging repository the plugin
#   reported creating. Used after a failed deploy, when the plugin may
#   already have deleted its properties file.
function maven_find_created_staging_repos {
  local deploy_log="${1:-${MAVEN_DEPLOY_LOG}}"

  created_staging_repo_ids=()
  if [[ -f "${deploy_log}" ]]; then
    local repo_id
    while IFS= read -r repo_id; do
      created_staging_repo_ids+=("${repo_id}")
    done < <(sed -n 's/.*Created staging repository with ID "\([^"]*\)".*/\1/p' "${deploy_log}")
  fi

  [[ ${#created_staging_repo_ids[@]} -gt 0 ]]
}

function maven_cleanup_settings {
  local settings_file="${1:-.release-settings.xml}"
  if [[ -f "${settings_file}" ]]; then
    rm -f "${settings_file}"
    print_info "Cleaned up Maven settings file"
  fi
}
