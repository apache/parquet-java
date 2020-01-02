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

################################################################################
# This is a branch-specific script that gets invoked at the end of
# travis-before_install.sh. It is run for the master branch only.
################################################################################

fail_the_build=
reduced_pom="$(tempfile)"
shopt -s globstar # Enables ** to match files in subdirectories recursively
for pom in **/pom.xml
do
  # Removes the project/version and project/parent/version elements, because
  # those are allowed to have SNAPSHOT in them. Also removes comments.
  xmlstarlet ed -N pom='http://maven.apache.org/POM/4.0.0' \
             -d '/pom:project/pom:version|/pom:project/pom:parent/pom:version|//comment()' "$pom" > "$reduced_pom"
  if grep -q SNAPSHOT "$reduced_pom"
  then
    if [[ ! "$fail_the_build" ]]
    then
      printf "Error: POM files in the master branch can not refer to SNAPSHOT versions.\n"
      fail_the_build=YES
    fi
    printf "\nOffending POM file: %s\nOffending content:\n" "$pom"
    # Removes every element that does not have SNAPSHOT in it or its
    # descendants. As a result, we get a skeleton of the POM file with only the
    # offending parts.
    xmlstarlet ed -d "//*[count((.|.//*)[contains(text(), 'SNAPSHOT')]) = 0]" "$reduced_pom"
  fi
done
rm "$reduced_pom"
if [[ "$fail_the_build" ]]
then
   exit 1
fi
