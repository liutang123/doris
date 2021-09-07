#!/usr/bin/env bash
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

set -x -e

WORKER_DIR=`pwd`
if [ ! -n "$DORIS_SRC_DIR" ]; then
  DORIS_SRC_DIR=/root/incubator-doris/
fi
if [ ! -n "$TEST_DIR" ]; then
  TEST_DIR=$DORIS_SRC_DIR/tests
fi
HOST_NAME=`hostname`
# deploy a doris cluster with one fe an one be
if [ ! -n "$FE_DIR" ]; then
  FE_DIR=$DORIS_SRC_DIR/output/fe
fi
if [ ! -n "$BE_DIR" ]; then
  BE_DIR=$DORIS_SRC_DIR/output/be
fi
# deploy fe
cd $FE_DIR
mkdir -p doris-meta
bash bin/start_fe.sh --daemon
doris-client fe check

# deploy be
cd $BE_DIR
mkdir -p storage
doris-client fe add_backend --be_host $HOST_NAME
bin/start_be.sh --daemon

doris-client fe check_backends

doris-client fe run_sql --sql 'CREATE DATABASE IF NOT EXISTS test'

# run functioinal tests
cd $TEST_DIR

# prepare test data
python3 prepare_ssb_sf02.py --data-path ssb_sf02
python3 doris-test.py

