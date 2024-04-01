#!/usr/bin/env bash
# shellcheck disable=2034

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

#################################################################################
# This script will
# 1. Check prerequisite libraries. Including:
#    cmake byacc flex automake libtool binutils-dev libiberty-dev bison
# 2. Compile and install all thirdparties which are downloaded
#    using *download-thirdparty.mt.sh*.
#
# This script will run *download-thirdparty.mt.sh* once again
# to check if all thirdparties have been downloaded, unpacked and patched.
#################################################################################

set -eo pipefail

curdir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

export DORIS_HOME="${curdir}/.."
export TP_DIR="${curdir}"

# include custom environment variables
if [[ -f "${DORIS_HOME}/env.sh" ]]; then
    export BUILD_THIRDPARTY_WIP=1
    . "${DORIS_HOME}/env.sh"
    export BUILD_THIRDPARTY_WIP=
fi

# Check args
usage() {
    echo "
Usage: $0 [options...] [packages...]
  Optional options:
     -j <num>               build thirdparty parallel
     --clean                clean the extracted data
     --continue <package>   continue to build the remaining packages (starts from the specified package)
  "
    exit 1
}

if ! OPTS="$(getopt \
    -n "$0" \
    -o 'hj:' \
    -l 'help,clean,continue:' \
    -- "$@")"; then
    usage
fi

eval set -- "${OPTS}"

KERNEL="$(uname -s)"

if [[ "${KERNEL}" == 'Darwin' ]]; then
    PARALLEL="$(($(sysctl -n hw.logicalcpu) / 4 + 1))"
else
    PARALLEL="$(($(nproc) / 4 + 1))"
fi

while true; do
    case "$1" in
    -j)
        PARALLEL="$2"
        shift 2
        ;;
    -h)
        HELP=1
        shift
        ;;
    --help)
        HELP=1
        shift
        ;;
    --clean)
        CLEAN=1
        shift
        ;;
    --continue)
        CONTINUE=1
        start_package="${2}"
        shift 2
        ;;
    --)
        shift
        break
        ;;
    *)
        echo "Internal error"
        exit 1
        ;;
    esac
done

if [[ "${CONTINUE}" -eq 1 ]]; then
    if [[ -z "${start_package}" ]] || [[ "${#}" -ne 0 ]]; then
        usage
    fi
fi

read -r -a packages <<<"${@}"

if [[ "${HELP}" -eq 1 ]]; then
    usage
fi

echo "Get params:
    PARALLEL            -- ${PARALLEL}
    CLEAN               -- ${CLEAN}
    PACKAGES            -- ${packages[*]}
    CONTINUE            -- ${start_package}
"

if [[ ! -f "${TP_DIR}/download-thirdparty.mt.sh" ]]; then
    echo "Download thirdparty script is missing".
    exit 1
fi

if [[ ! -f "${TP_DIR}/vars.mt.sh" ]]; then
    echo "vars.sh is missing".
    exit 1
fi

. "${TP_DIR}/vars.mt.sh"

cd "${TP_DIR}"

if [[ "${CLEAN}" -eq 1 ]] && [[ -d "${TP_SOURCE_DIR}" ]]; then
    echo 'Clean the extracted data ...'
    find "${TP_SOURCE_DIR}" -mindepth 1 -maxdepth 1 -type d -exec rm -rf {} \;
    echo 'Success!'
fi


# Download thirdparties.
"${TP_DIR}/download-thirdparty.mt.sh"

export LD_LIBRARY_PATH="${TP_DIR}/installed/lib:${LD_LIBRARY_PATH}"

# toolchain specific warning options and settings
if [[ "${CC}" == *gcc ]]; then
    warning_uninitialized='-Wno-maybe-uninitialized'
    warning_stringop_truncation='-Wno-stringop-truncation'
    warning_class_memaccess='-Wno-class-memaccess'
    warning_array_parameter='-Wno-array-parameter'
    warning_narrowing='-Wno-narrowing'
    warning_dangling_reference='-Wno-dangling-reference'
    boost_toolset='gcc'
elif [[ "${CC}" == *clang ]]; then
    warning_uninitialized='-Wno-uninitialized'
    warning_shadow='-Wno-shadow'
    warning_dangling_gsl='-Wno-dangling-gsl'
    warning_unused_but_set_variable='-Wno-unused-but-set-variable'
    warning_defaulted_function_deleted='-Wno-defaulted-function-deleted'
    warning_reserved_identifier='-Wno-reserved-identifier'
    warning_suggest_override='-Wno-suggest-override -Wno-suggest-destructor-override'
    warning_option_ignored='-Wno-option-ignored'
    warning_narrowing='-Wno-c++11-narrowing'
    boost_toolset='clang'
    libhdfs_cxx17='-std=c++1z'

    test_warning_result="$("${CC}" -xc++ "${warning_unused_but_set_variable}" /dev/null 2>&1 || true)"
    if echo "${test_warning_result}" | grep 'unknown warning option' >/dev/null; then
        warning_unused_but_set_variable=''
    fi
fi

# prepare installed prefix
mkdir -p "${TP_DIR}/installed/lib64"
pushd "${TP_DIR}/installed"/
ln -sf lib64 lib
popd

# Configure the search paths for pkg-config and cmake
export PKG_CONFIG_PATH="${TP_DIR}/installed/lib64/pkgconfig"
export CMAKE_PREFIX_PATH="${TP_DIR}/installed"

echo "PKG_CONFIG_PATH: ${PKG_CONFIG_PATH}"
echo "CMAKE_PREFIX_PATH: ${CMAKE_PREFIX_PATH}"

check_prerequest() {
    local CMD="$1"
    local NAME="$2"
    if ! eval "${CMD}"; then
        echo "${NAME} is missing"
        exit 1
    else
        echo "${NAME} is found"
    fi
}

# sudo apt-get install cmake
# sudo yum install cmake
check_prerequest "${CMAKE_CMD} --version" "cmake"

# sudo apt-get install byacc
# sudo yum install byacc
check_prerequest "byacc -V" "byacc"

# sudo apt-get install flex
# sudo yum install flex
check_prerequest "flex -V" "flex"

# sudo apt-get install automake
# sudo yum install automake
check_prerequest "automake --version" "automake"

# sudo apt-get install libtool
# sudo yum install libtool
check_prerequest "libtoolize --version" "libtool"

# aclocal_version should equal to automake_version
aclocal_version=$(aclocal --version | sed -n '1p' | awk 'NF>1{print $NF}')
automake_version=$(automake --version | sed -n '1p' | awk 'NF>1{print $NF}')
if [[ "${aclocal_version}" != "${automake_version}" ]]; then
    echo "Error: aclocal version(${aclocal_version}) is not equal to automake version(${automake_version})."
    exit 1
fi

# sudo apt-get install binutils-dev
# sudo yum install binutils-devel
#check_prerequest "locate libbfd.a" "binutils-dev"

# sudo apt-get install libiberty-dev
# no need in centos 7.1
#check_prerequest "locate libiberty.a" "libiberty-dev"

# sudo apt-get install bison
# sudo yum install bison
# necessary only when compiling be
#check_prerequest "bison --version" "bison"

#########################
# build all thirdparties
#########################

# Name of cmake build directory in each thirdpary project.
# Do not use `build`, because many projects contained a file named `BUILD`
# and if the filesystem is not case sensitive, `mkdir` will fail.
BUILD_DIR=doris_build

check_if_source_exist() {
    if [[ -z $1 ]]; then
        echo "dir should specified to check if exist."
        exit 1
    fi

    if [[ ! -d "${TP_SOURCE_DIR}/$1" ]]; then
        echo "${TP_SOURCE_DIR}/$1 does not exist."
        exit 1
    fi
    echo "===== begin build $1"
}

check_if_archive_exist() {
    if [[ -z $1 ]]; then
        echo "archive should specified to check if exist."
        exit 1
    fi

    if [[ ! -f "${TP_SOURCE_DIR}/$1" ]]; then
        echo "${TP_SOURCE_DIR}/$1 does not exist."
        exit 1
    fi
}

remove_all_dylib() {
    if [[ "${KERNEL}" == 'Darwin' ]]; then
        find "${TP_INSTALL_DIR}/lib64" -name "*.dylib" -delete
    fi
}

if [[ -z "${STRIP_TP_LIB}" ]]; then
    if [[ "${KERNEL}" != 'Darwin' ]]; then
        STRIP_TP_LIB='ON'
    else
        STRIP_TP_LIB='OFF'
    fi
fi

if [[ "${STRIP_TP_LIB}" = "ON" ]]; then
    echo "Strip thirdparty libraries"
else
    echo "Do not strip thirdparty libraries"
fi

strip_lib() {
    if [[ "${STRIP_TP_LIB}" = "ON" ]]; then
        if [[ -z $1 ]]; then
            echo "Must specify the library to be stripped."
            exit 1
        fi
        if [[ ! -f "${TP_LIB_DIR}/$1" ]]; then
            echo "Library to be stripped (${TP_LIB_DIR}/$1) does not exist."
            exit 1
        fi
        strip --strip-debug --strip-unneeded "${TP_LIB_DIR}/$1"
    fi
}

# spdlog
build_spdlog() {
    check_if_source_exist $SPDLOG_SOURCE
    cd $TP_SOURCE_DIR/$SPDLOG_SOURCE
    mkdir build -p && cd build
    rm -rf CMakeCache.txt CMakeFiles/
    $CMAKE_CMD -DCMAKE_INSTALL_PREFIX=$TP_INSTALL_DIR -DSPDLOG_BUILD_BENCH=OFF -DSPDLOG_BUILD_TESTS=OF ../
    make -j$PARALLEL && make install
}

# croaring bitmap build for MT doris
build_croaringbitmap() {
    avx_flag=''
    if [[ -n "${USE_AVX2}" && "${USE_AVX2}" -eq 0 ]]; then
        echo "set USE_AVX2=${USE_AVX2} to FORCE disable AVX2 in croaringbitmap"
        avx_flag="-DROARING_DISABLE_AVX=ON"
    fi

    check_if_source_exist "${CROARINGBITMAP_SOURCE}"
    cd "${TP_SOURCE_DIR}/${CROARINGBITMAP_SOURCE}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    rm -rf CMakeCache.txt CMakeFiles/

    if [[ "${KERNEL}" != 'Darwin' ]]; then
        ldflags="-L${TP_LIB_DIR} -static-libstdc++ -static-libgcc"
    else
        ldflags="-L${TP_LIB_DIR}"
    fi

    CXXFLAGS="-O3" \
        LDFLAGS="${ldflags}" \
        "${CMAKE_CMD}" -G "${GENERATOR}" ${avx_flag:+${avx_flag}} -DROARING_BUILD_STATIC=ON -DCMAKE_INSTALL_PREFIX="${TP_INSTALL_DIR}" \
        -DENABLE_ROARING_TESTS=OFF ..

    "${BUILD_SYSTEM}" -j "${PARALLEL}"
    "${BUILD_SYSTEM}" install
}

build_spdlog
build_croaringbitmap

echo "Finished to build all mt thirdparties"


