#!/bin/bash -l
set -x
set -e  # 一旦遇到非0退出执行，否则遇到失败继续执行流水线状态仍显示成功
# bci-uploads3命令功能：将包上传S3和unity

pwd

# 获取参数值
build_type=$1
module=$2
tag=$3
images=$4
current_path=`pwd`
# task_id 为了使流水线执行时, 保证创建的容器唯一避免容器冲突导致流水线失败, 流水线中传递的值是流水线任务ID;也可以传递其他值
task_id=$5
commitid=$6

# 确定打bin包使用的commitid
if [[ "$commitid" == "" ]]; then
    commit_id=`git rev-parse HEAD`
    version=${commit_id:0:11}
else
    version=$commitid
fi


fe_package_name="doris-fe_bin_default_${version}.tar.gz"
if [[ "$build_type" != "Release" ]]; then
    be_package_name="doris-be_bin_${build_type}_${version}.tar.gz"
else
    be_package_name="doris-be_bin_default_${version}.tar.gz"
fi
broker_package_name="doris-broker_bin_default_${version}.tar.gz"
spark_dpp_package_name="spark-dpp_bin_default_${version}.tar.gz"

container_name='210-build-'$version$task_id

function build_fe() {
    docker run \
    -w /home/sankuai/doris \
    --name $container_name \
    --rm \
    -i \
    -e JAVA_HOME=/usr/local/java12 \
    -e CUSTOM_NPM_REGISTRY=http://r.npm.sankuai.com \
    -e USE_AVX2=$tag \
    -e TZ=Asia/Shanghai \
    -e DISABLE_JAVA_CHECK_STYLE=ON \
    -v `pwd`:/home/sankuai/doris \
    -v ~/.m2:/home/sankuai/.m2 \
    -u sankuai \
    $images \
    /bin/bash ./build.sh --clean --fe
}

# Only Java8 is supported in BE.
function build_be() {
    docker run \
    -w /home/sankuai/doris \
    --name $container_name \
    --rm \
    -i \
    -e JAVA_HOME=/usr/local/jdk1.8.0_112 \
    -e CUSTOM_NPM_REGISTRY=http://r.npm.sankuai.com \
    -e USE_AVX2=$tag \
    -e BUILD_TYPE=$build_type \
    -e DISABLE_JAVA_CHECK_STYLE=ON \
    -e TZ=Asia/Shanghai \
    -v `pwd`:/home/sankuai/doris \
    -v ~/.m2:/home/sankuai/.m2 \
    -u sankuai \
    $images \
    /bin/bash ./build.sh --clean --be
}

function build_spark_dpp() {
    docker run \
    -w /home/sankuai/doris \
    --name $container_name \
    --rm \
    -i \
    -e JAVA_HOME=/usr/local/jdk1.8.0_112 \
    -e CUSTOM_NPM_REGISTRY=http://r.npm.sankuai.com \
    -e USE_AVX2=$tag \
    -e DISABLE_JAVA_CHECK_STYLE=ON \
    -e TZ=Asia/Shanghai \
    -v `pwd`:/home/sankuai/doris -v \
    ~/.m2:/home/sankuai/.m2 \
    -u sankuai \
    $images \
    /bin/bash ./build.sh --clean --spark-dpp
}

function build_broker() {
    docker run \
    -w /home/sankuai/doris \
    --name $container_name \
    --rm \
    -i \
    -e JAVA_HOME=/usr/local/jdk1.8.0_112 \
    -e CUSTOM_NPM_REGISTRY=http://r.npm.sankuai.com \
    -e USE_AVX2=$tag \
    -e TZ=Asia/Shanghai \
    -e DISABLE_JAVA_CHECK_STYLE=ON \
    -v `pwd`:/home/sankuai/doris \
    -v ~/.m2:/home/sankuai/.m2 \
    -u sankuai \
    $images \
    /bin/bash -c "cd fs_brokers/apache_hdfs_broker && ./build.sh"
}

if [[ "$module" == "doris-fe" ]]; then
    # step1 执行编译命令
    # 改为 docker run 启动容器
    build_fe
    build_spark_dpp
    # step2 打包
    cd output/fe
    tar -zcvf $fe_package_name lib spark-dpp webroot bin mysql_ssl_default_certificate minidump
    # step3 上传S3/unity
    bci-uploads3 -c doris-fe -p ./$fe_package_name -f True
elif [[ "$module" == "doris-be" ]]; then
    build_be
    cd output/be
    [[ -d lib/meta_tool ]] && rm -rf lib/meta_tool
    tar -zcvf $be_package_name lib bin www dict zoneinfo
    bci-uploads3 -c doris-be -p ./$be_package_name -f True
elif [[ "$module" == "all" ]]; then
    build_be
    build_fe
    build_spark_dpp
    # step2.1 fe打包
    cd output/fe
    tar -zcvf $fe_package_name lib spark-dpp webroot bin mysql_ssl_default_certificate minidump
    # step3 上传S3/unity
    bci-uploads3 -c doris-fe -p ./$fe_package_name -f True
    # 单独打spark-dpp包
    tar -zcvf $spark_dpp_package_name spark-dpp
    bci-uploads3 -c doris-fe -p ./$spark_dpp_package_name -f True
    # step2.2 be打包
    cd $current_path/output/be
    [[ -d lib/meta_tool ]] && rm -rf lib/meta_tool
    tar -zcvf $be_package_name lib bin www dict zoneinfo
    bci-uploads3 -c doris-be -p ./$be_package_name -f True
elif [[ "$module" == "broker" ]]; then
    build_broker
    cd $current_path/fs_brokers/apache_hdfs_broker/output/
    tar -zcvf $broker_package_name apache_hdfs_broker
    bci-uploads3 -c doris-broker -p ./$broker_package_name -f True
elif [[ "$module" == "spark-dpp" ]]; then
    build_fe
    build_spark_dpp
    cd output/fe
    tar -zcvf $spark_dpp_package_name spark-dpp
    bci-uploads3 -c doris-fe -p ./$spark_dpp_package_name -f True
fi