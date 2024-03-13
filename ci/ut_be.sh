#!/bin/bash
set -x
set -e

pwd

commit_id=`git rev-parse HEAD`
version=${commit_id:0:11}

#### 单测开始执行 ####
name='210-pipeline_be_ut-'
container_is_run=`docker ps -a | grep "$name$version" | awk '{print $NF}'`
if [[ "$container_is_run" == "$name$version" ]]; then
  docker rm -f $name$version
fi
image='registryonline-hulk.sankuai.com/custom_prod/com.sankuai.dataqa.log.scribesdk/doris_package:build-mt-2.1.0'
docker run --privileged -w /home/sankuai/doris --name $name$version --rm -i -e DISABLE_JAVA_CHECK_STYLE=ON -e JAVA_HOME=/usr/local/java12 -e TZ=Asia/Shanghai -v `pwd`:/home/sankuai/doris -v ~/.m2:/home/sankuai/.m2 -u sankuai $image /bin/bash run-be-ut.sh --clean --run
