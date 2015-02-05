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

OAUTH_FILE=~/.github_oauth_for_changelog
if [ -f $OAUTH_FILE ]
then
  token=`cat $OAUTH_FILE`
else
  echo "Please create an oauth token here: https://github.com/settings/tokens/new"
  echo "Then paste it bellow (it will be saved in $OAUTH_FILE):" >&2
  read token >&2
  echo $token > $OAUTH_FILE
  chmod og-rwx $OAUTH_FILE
fi
TOKEN_HEADER="Authorization: token $token"

curl -f -H "$TOKEN_HEADER" -s "https://api.github.com" > /dev/null
if [ $? == 0 ]
then
  echo "login successful" >&2
else
  echo "login failed" >&2
  curl -H "$TOKEN_HEADER" -s "https://api.github.com"
  echo "if your OAUTH token needs to be replaced you can delete file $OAUTH_FILE"
  exit 1
fi

echo "# Parquet #"

git log | grep -E "Merge pull request|prepare release" | while read l
do 
  release=`echo $l | grep "\[maven-release-plugin\] prepare release" | cut -d "-" -f 4`
  PR=`echo $l| grep -E -o "Merge pull request #[^ ]*" | cut -d "#" -f 2`
#  echo $l
  if [ -n "$release" ] 
  then 
    echo
    echo "### Version $release ###"
  fi
  if [ -n "$PR" ]
  then
    JSON=`curl -H "$TOKEN_HEADER" -s https://api.github.com/repos/Parquet/parquet-mr/pulls/$PR | tr "\n" " "`
    DESC_RAW=$(echo $JSON |  grep -Eo '"title":.*?[^\\]",' | cut -d "\"" -f 4- | head -n 1 | sed -e "s/\\\\//g")
    DESC=$(echo ${DESC_RAW%\",})
    echo "* ISSUE [$PR](https://github.com/Parquet/parquet-mr/pull/$PR): ${DESC}"
  fi
done

