#!/usr/bin/env bash
#set -x

curdir=$(dirname "$0")
curdir=$(
  cd "$curdir"
  pwd
)
WORK_DIR=$curdir

JAR_DIR="${WORK_DIR}/cdw-doris-release/need_to_install_jar"
mvn install:install-file  \
  -DgroupId=org.apache.doris  \
  -DartifactId=je \
  -Dversion=18.3.14-doris-SNAPSHOT  \
  -Dpackaging=jar  \
  -Dfile=${JAR_DIR}/je-18.3.14-doris-SNAPSHOT.jar

# clean and build all
export JAVA_HOME=/usr/lib/jvm/jdk-17.0.2/
export PATH=$JAVA_HOME/bin/:$PATH
PARALLEL=$(nproc)
sh ${WORK_DIR}/build.sh --clean --fe --be --cloud --broker  --spark-dpp --hive-udf -j${PARALLEL}
if [ $? -ne 0 ]; then
  echo "build fe be and ui failed!"
  exit 1
fi

