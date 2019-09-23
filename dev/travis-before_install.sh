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
# This script gets invoked by .travis.yml in the before_install step
################################################################################

export THIFT_VERSION=0.12.0



set -e

date

sudo apt-get update -qq
sudo apt-get install -qq build-essential pv autoconf automake libtool curl make \
   g++ unzip libboost-dev libboost-test-dev libboost-program-options-dev \
   libevent-dev automake libtool flex bison pkg-config g++ libssl-dev xmlstarlet
date
pwd
wget -nv https://archive.apache.org/dist/thrift/${THIFT_VERSION}/thrift-${THIFT_VERSION}.tar.gz
tar zxf thrift-${THIFT_VERSION}.tar.gz
cd thrift-${THIFT_VERSION}
chmod +x ./configure
./configure --disable-gen-erl --disable-gen-hs --without-c-glib --without-erlang --without-go --without-haskell --without-java --without-nodejs --without-php --without-python --without-ruby

sudo make install
cd ..
branch_specific_script="dev/travis-before_install-${TRAVIS_BRANCH}.sh"
if [[ -e "$branch_specific_script" ]]
then
  . "$branch_specific_script"
fi
date

