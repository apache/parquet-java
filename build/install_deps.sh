#!/usr/bin/env bash

set -o pipefail
set -x
set -eu

case $(uname) in
  'Linux')
    # protoc is already available on circle nodes. Need to ensure version didn't change
    wget -O thrift-compiler_0.9.1-2.deb http://ftp.us.debian.org/debian/pool/main/t/thrift-compiler/thrift-compiler_0.9.1-2_amd64.deb
    sudo dpkg -i thrift-compiler_0.9.1-2.deb
    rm thrift-compiler_0.9.1-2.deb
    ;;
  'Darwin')
    brew tap homebrew/versions
    brew install protobuf250
    brew install thrift
    ;;
  *) ;;
esac

