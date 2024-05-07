#!/usr/bin/env bash
#set -x

curdir=$(dirname "$0")
curdir=$(
  cd "$curdir"
  pwd
)
WORK_DIR=$curdir

# clean and build all
PARALLEL=$(nproc)
sh ${WORK_DIR}/build.sh --clean --fe --be --broker  --spark-dpp --hive-udf -j${PARALLEL}
if [ $? -ne 0 ]; then
  echo "build fe be and ui failed!"
  exit 1
fi

# build and install audit loader plugin
# avoid cannnot find symbal issue
cd  ${WORK_DIR}/fe
mvn install -DskipTests=true

cd ${WORK_DIR}/fe_plugins/auditloader
sh build.sh
cd -
