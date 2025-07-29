#!/usr/bin/env bash
#set -x

curdir=$(dirname "$0")
curdir=$(
  cd "$curdir"
  pwd
)
WORK_DIR=$curdir

mvn install:install-file \
  -Dfile=setats-uber-shaded-0.5-SNAPSHOT.jar \
  -DgroupId=com.tencent.oceanus \
  -DartifactId=setats-uber-shaded \
  -Dversion=0.5-SNAPSHOT \
  -Dpackaging=jar


mvn install:install-file \
  -Dfile=setats-uber-shaded-0.5-SNAPSHOT.jar \
  -DgroupId=com.tencent.oceanus \
  -DartifactId=streaming-iceberg-parent \
  -Dversion=0.5-SNAPSHOT \
  -Dpackaging=pom

# clean and build all
export JAVA_HOME=/usr/lib/jvm/jdk-17.0.2/
export PATH=$JAVA_HOME/bin/:$PATH
PARALLEL=$(nproc)
sh ${WORK_DIR}/build.sh --clean --fe --be --cloud --broker  --spark-dpp --hive-udf -j${PARALLEL}
if [ $? -ne 0 ]; then
  echo "build fe be and ui failed!"
  exit 1
fi

