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

set -eo pipefail

curdir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

DORIS_HOME="$(
    cd "${curdir}/.." || exit 1
    pwd
)"

cd "${DORIS_HOME}" || exit 1

if [[ ! -d bin || ! -d conf || ! -d lib ]]; then
    echo "$0 must be invoked at the directory which contains bin, conf and lib"
    exit 1
fi

RUN_DAEMON=0
RUN_VERSION=0
RUN_CONSOLE=0
RUN_METASERVICE=0
RUN_RECYCLYER=0
for arg; do
    shift
    [[ "${arg}" = "--daemonized" ]] && RUN_DAEMON=1 && continue
    [[ "${arg}" = "-daemonized" ]] && RUN_DAEMON=1 && continue
    [[ "${arg}" = "--daemon" ]] && RUN_DAEMON=1 && continue
    [[ "${arg}" = "--version" ]] && RUN_VERSION=1 && continue
    [[ "${arg}" = "--console" ]] && RUN_CONSOLE=1 && continue
    [[ "${arg}" = "--meta-service" ]] && RUN_METASERVICE=1 && continue
    [[ "${arg}" = "--recycler" ]] && RUN_RECYCLYER=1 && continue
    set -- "$@" "${arg}"
done
if [[ ${RUN_METASERVICE} -eq 1 ]]; then
    set -- "$@" "--meta-service"
fi
if [[ ${RUN_RECYCLYER} -eq 1 ]]; then
    set -- "$@" "--recycler"
fi
# echo "$@" "daemonized=${daemonized}"}

# export env variables from doris_cloud.conf
# read from doris_cloud.conf
while read -r line; do
    envline="$(echo "${line}" |
        sed 's/[[:blank:]]*=[[:blank:]]*/=/g' |
        sed 's/^[[:blank:]]*//g' |
        grep -E "^[[:upper:]]([[:upper:]]|_|[[:digit:]])*=" ||
        true)"
    envline="$(eval "echo ${envline}")"
    if [[ "${envline}" == *"="* ]]; then
        eval 'export "${envline}"'
    fi
done <"${DORIS_HOME}/conf/doris_cloud.conf"

STDOUT_LOGGER="${LOG_DIR}/doris_cloud.out"
log() {
    # same datetime format as in fe.log: 2024-06-03 14:54:41,478
    cur_date=$(date +"%Y-%m-%d %H:%M:%S,$(date +%3N)")
    if [[ "${RUN_CONSOLE}" -eq 1 ]]; then
        echo "StdoutLogger ${cur_date} $1"
    else
        echo "StdoutLogger ${cur_date} $1" >>"${STDOUT_LOGGER}"
    fi
}

role=''
if [[ ${RUN_METASERVICE} -eq 0 ]] && [[ ${RUN_RECYCLYER} -eq 0 ]]; then
    role='MetaService and Recycler'
elif [[ ${RUN_METASERVICE} -eq 1 ]] && [[ ${RUN_RECYCLYER} -eq 0 ]]; then
    role='MetaService'
elif [[ ${RUN_METASERVICE} -eq 0 ]] && [[ ${RUN_RECYCLYER} -eq 1 ]]; then
    role='Recycler'
elif [[ ${RUN_METASERVICE} -eq 1 ]] && [[ ${RUN_RECYCLYER} -eq 1 ]]; then
    role='MetaService and Recycler'
fi

pidfile="${PID_DIR}/doris_cloud.pid"
if [[ ${RUN_VERSION} -eq 0 ]] && [[ -f "${pidfile}" ]]; then
    pid=$(cat "${pidfile}")
    if [[ "${pid}" != "" ]]; then
        if kill -0 "$(cat "${pidfile}")" >/dev/null 2>&1; then
            echo "pid file existed, ${role} have already started, pid=${pid}"
            exit 1
        fi
    fi
    echo "pid file existed but process not alive, remove it, pid=${pid}"
    rm -f "${pidfile}"
fi

lib_path="${DORIS_HOME}/lib/ms"
bin="${DORIS_HOME}/lib/ms/doris_cloud"
export LD_LIBRARY_PATH="${lib_path}:${LD_LIBRARY_PATH}"

JAVA_HOME=/usr/local/jdk17
if [ ! -d "$JAVA_HOME" ]; then
    JAVA_HOME=/usr/local/jdk
fi
export JAVA_HOME

if [[ -d "${DORIS_HOME}/lib/hadoop_hdfs/" ]]; then
    # add hadoop libs
    for f in "${DORIS_HOME}/lib/hadoop_hdfs/common"/*.jar; do
        DORIS_CLASSPATH="${DORIS_CLASSPATH}:${f}"
    done
    for f in "${DORIS_HOME}/lib/hadoop_hdfs/common/lib"/*.jar; do
        DORIS_CLASSPATH="${DORIS_CLASSPATH}:${f}"
    done
    for f in "${DORIS_HOME}/lib/hadoop_hdfs/hdfs"/*.jar; do
        DORIS_CLASSPATH="${DORIS_CLASSPATH}:${f}"
    done
    for f in "${DORIS_HOME}/lib/hadoop_hdfs/hdfs/lib"/*.jar; do
        DORIS_CLASSPATH="${DORIS_CLASSPATH}:${f}"
    done
fi

export CLASSPATH="${DORIS_CLASSPATH}"

export LD_LIBRARY_PATH="${JAVA_HOME}/lib/server:${JAVA_HOME}/lib:${LD_LIBRARY_PATH}"

## set libhdfs3 conf
if [[ -f "${DORIS_HOME}/conf/hdfs-site.xml" ]]; then
    export LIBHDFS3_CONF="${DORIS_HOME}/conf/hdfs-site.xml"
fi

jdk_version() {
    local java_cmd="${1}"
    local result
    local IFS=$'\n'

    if ! command -v "${java_cmd}" >/dev/null; then
        echo "ERROR: invalid java_cmd ${java_cmd}" >>"${STDOUT_LOGGER}"
        result=no_java
        return 1
    else
        echo "INFO: java_cmd ${java_cmd}" >>"${STDOUT_LOGGER}"
        local version
        # remove \r for Cygwin
        version="$("${java_cmd}" -Xms32M -Xmx32M -version 2>&1 | tr '\r' '\n' | grep version | awk '{print $3}')"
        version="${version//\"/}"
        if [[ "${version}" =~ ^1\. ]]; then
            result="$(echo "${version}" | awk -F '.' '{print $2}')"
        else
            result="$(echo "${version}" | awk -F '.' '{print $1}')"
        fi
        echo "INFO: jdk_version ${result}" >>"${STDOUT_LOGGER}"
    fi
    echo "${result}"
    return 0
}

# echo "LIBHDFS3_CONF=${LIBHDFS3_CONF}"
# check java version and choose correct JAVA_OPTS
java_version="$(
    set -e
    jdk_version "${JAVA_HOME}/bin/java"
)"

CUR_DATE=$(date +%Y%m%d-%H%M%S)
LOG_PATH="-DlogPath=${LOG_DIR}/jni.log"
COMMON_OPTS="-Dsun.java.command=DorisCloud"
JDBC_OPTS="-DJDBC_MIN_POOL=1 -DJDBC_MAX_POOL=100 -DJDBC_MAX_IDLE_TIME=300000 -DJDBC_MAX_WAIT_TIME=5000"
if [[ "${java_version}" -eq 17 ]]; then
    if [[ -z ${JAVA_OPTS_FOR_JDK_17} ]]; then
        JAVA_OPTS_FOR_JDK_17="-Xmx4096m ${LOG_PATH} -Xlog:gc:${LOG_DIR}/ms.gc.log.${CUR_DATE} ${COMMON_OPTS} ${JDBC_OPTS} --add-opens=java.base/java.net=ALL-UNNAMED"
    fi
    final_java_opt="${JAVA_OPTS_FOR_JDK_17}"
else
    echo "ERROR: The jdk_version is ${java_version}, it must be 17." >>"${LOG_DIR}/doris_cloud.out"
    exit 1
fi

if [[ "${MACHINE_OS}" == "Darwin" ]]; then
    max_fd_limit='-XX:-MaxFDLimit'

    if ! echo "${final_java_opt}" | grep "${max_fd_limit/-/\\-}" >/dev/null; then
        final_java_opt="${final_java_opt} ${max_fd_limit}"
    fi

    if [[ -n "${JAVA_OPTS_FOR_JDK_17}" ]] && ! echo "${JAVA_OPTS_FOR_JDK_17}" | grep "${max_fd_limit/-/\\-}" >/dev/null; then
        export JAVA_OPTS="${JAVA_OPTS_FOR_JDK_17} ${max_fd_limit}"
    fi
fi

# set LIBHDFS_OPTS for hadoop libhdfs
export LIBHDFS_OPTS="${final_java_opt}"

# to enable dump jeprof heap stats prodigally, change `prof_active:false` to `prof_active:true` or curl http://be_host:be_webport/jeheap/prof/true
# to control the dump interval change `lg_prof_interval` to a specific value, it is pow/exponent of 2 in size of bytes, default 34 means 2 ** 34 = 16GB
# to control the dump path, change `prof_prefix` to a specific path, e.g. /doris_cloud/log/ms_, by default it dumps at the path where the start command called
export JEMALLOC_CONF="percpu_arena:percpu,background_thread:true,metadata_thp:auto,muzzy_decay_ms:5000,dirty_decay_ms:5000,oversize_threshold:0,prof_prefix:ms_,prof:true,prof_active:false,lg_prof_interval:34"

if [[ "${RUN_VERSION}" -ne 0 ]]; then
    "${bin}" --version
    exit 0
fi

mkdir -p "${DORIS_HOME}/log"
echo "$(date +'%F %T') start with args: $*"
if [[ "${RUN_DAEMON}" -eq 1 ]]; then
    # append 10 blank lines to ensure the following tail -n10 works correctly
    printf "\n\n\n\n\n\n\n\n\n\n" >>"${STDOUT_LOGGER}"
    echo "$(date +'%F %T') start with args: $*" >>"${STDOUT_LOGGER}"
    nohup "${bin}" "$@" >>"${STDOUT_LOGGER}" 2>&1 &
    echo "wait and check ${role} start successfully" >>"${STDOUT_LOGGER}"
    sleep 3
    tail -n12 "${STDOUT_LOGGER}" | grep 'successfully started service'
    ret=$?
    if [[ ${ret} -ne 0 ]]; then
        echo "${role} may not start successfully please check process log for more details"
        exit 1
    fi
    tail -n12 "${STDOUT_LOGGER}"
    exit 0
elif [[ "${RUN_CONSOLE}" -eq 1 ]]; then
    export DORIS_LOG_TO_STDERR=1
    "${bin}" "$@" 2>&1
else
    "${bin}" "$@"
fi

# vim: et ts=2 sw=2:
