#!/bin/bash
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

############################################################
# You may have to set variables bellow,
# which are used for compiling thirdparties and palo itself.
############################################################

###################################################
# DO NOT change variables bellow unless you known 
# what you are doing.
###################################################

# thirdparties will be downloaded and unpacked here
export TP_SOURCE_DIR=$TP_DIR/src

# thirdparties will be installed to here
# MT set install
if [[ -z ${TP_INSTALL_DIR} ]]; then
    export TP_INSTALL_DIR=$TP_DIR/installed
fi

# patches for all thirdparties
export TP_PATCH_DIR=$TP_DIR/patches

# header files of all thirdparties will be intalled to here
export TP_INCLUDE_DIR=$TP_INSTALL_DIR/include

# libraries of all thirdparties will be intalled to here
export TP_LIB_DIR=$TP_INSTALL_DIR/lib

# all java libraries will be unpacked to here
export TP_JAR_DIR=$TP_INSTALL_DIR/lib/jar

#####################################################
# Download url, filename and unpaced filename
# of all thirdparties
#####################################################

# spdlog
SPDLOG_DOWNLOAD="https://s3plus.sankuai.com/v1/mss_aee445ae7aa0438c82eacce6e3f6cb2c/doris-functionaltest/thirdparty/v1.7.0.tar.gz"
SPDLOG_NAME="spdlog-1.7.0.tar.gz"
SPDLOG_SOURCE="spdlog-1.7.0"
SPDLOG_MD5SUM="2e1c0c45920a3536366d2232963bc24a"

# roaring bitmap for MT
CROARINGBITMAP_DOWNLOAD="https://s3plus.sankuai.com/v1/mss_aee445ae7aa0438c82eacce6e3f6cb2c/doris-functionaltest/thirdparty/CRoaring-2.1.2.tar.gz"
CROARINGBITMAP_NAME=CRoaring-2.1.2.tar.gz
CROARINGBITMAP_SOURCE=CRoaring-2.1.2
CROARINGBITMAP_MD5SUM="a7385d8db78b0d758a9ec6cfa8e9db38"


# all thirdparties which need to be downloaded is set in array TP_ARCHIVES
export TP_ARCHIVES="
SPDLOG
CROARINGBITMAP
"

