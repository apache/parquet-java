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

set -e

if [ -z "$2" ]; then
    cat <<EOF
Usage: $0 <release-version> <new-development-version-without-SNAPSHOT-suffix>
Example: $0 1.6.0 1.7.0
EOF
  exit 1
fi

release_version="$1"
new_development_version="$2-SNAPSHOT"

tag="apache-parquet-$release_version"

mvn release:clean
mvn release:prepare -Dtag="$tag" "-DreleaseVersion=$release_version" -DdevelopmentVersion="$new_development_version"

echo "Finish staging binary artifacts by running: mvn release:perform"
