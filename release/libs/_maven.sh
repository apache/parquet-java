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

function _xml_escape {
  local str="$1"
  str="${str//&/&amp;}"
  str="${str//</&lt;}"
  str="${str//>/&gt;}"
  str="${str//\"/&quot;}"
  str="${str//\'/&apos;}"
  echo "${str}"
}

function generate_maven_settings {
  local settings_file="${1:-.release-settings.xml}"

  if [[ -z "${NEXUS_USERNAME:-}" || -z "${NEXUS_PASSWORD:-}" ]]; then
    print_warning "NEXUS_USERNAME or NEXUS_PASSWORD not set; Maven deploy may fail"
  fi

  local esc_username esc_password
  esc_username=$(_xml_escape "${NEXUS_USERNAME:-}")
  esc_password=$(_xml_escape "${NEXUS_PASSWORD:-}")

  (
    umask 077
    cat > "${settings_file}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.2.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.2.0 https://maven.apache.org/xsd/settings-1.2.0.xsd">
  <servers>
    <server>
      <id>apache.releases.https</id>
      <username>${esc_username}</username>
      <password>${esc_password}</password>
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
EOF
  )

  print_info "Generated Maven settings at ${settings_file} (mode 600)"
}

function maven_deploy {
  local settings_file="${1:-.release-settings.xml}"

  if [[ ! -f "${settings_file}" ]]; then
    print_info "Generating Maven settings..."
    generate_maven_settings "${settings_file}"
  fi

  exec_process ./mvnw deploy \
    -Papache-release \
    -DskipTests \
    -Darguments=-DskipTests \
    --settings "${settings_file}" \
    --batch-mode
}

function maven_cleanup_settings {
  local settings_file="${1:-.release-settings.xml}"
  if [[ -f "${settings_file}" ]]; then
    rm -f "${settings_file}"
    print_info "Cleaned up Maven settings file"
  fi
}
