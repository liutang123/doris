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
# 如何在mt内部编译docker image
# ├── jdk-12
# ├── palo
# │   ├── docker
# |   |   ├── Dockerfile.mt
# docker build -t registryonline-hulk.sankuai.com/custom_prod/com.sankuai.dataqa.log.scribesdk/doris_package:build-mt-2.1.0 -f palo/docker/compilation/Dockerfile.mt .
# 运行
# docker run -e DISABLE_JAVA_CHECK_STYLE=ON -e JAVA_HOME=/usr/local/java12 -e TZ=Asia/Shanghai -u sankuai -w /home/sankuai/doris --name 210-build --rm -it -v [doris code path]:/home/sankuai/doris \
# -v ~/.m2:/home/sankuai/.m2 registryonline-hulk.sankuai.com/custom_prod/com.sankuai.dataqa.log.scribesdk/doris_package:build-mt-2.1.0 \
# /bin/bash ./build.sh --clean --be --fe --ui

FROM apache/doris:build-env-ldb-toolchain-latest
ENV REPOSITORY_URL=https://s3plus.sankuai.com/v1/mss_aee445ae7aa0438c82eacce6e3f6cb2c/doris-functionaltest/thirdparty
ENV DEFAULT_DIR=/var/local
ENV TP_INSTALL_DIR=${DEFAULT_DIR}/thirdparty/installed
# clone lastest source code, download and build third party
COPY palo/thirdparty ${DEFAULT_DIR}/palo/thirdparty
COPY palo/env.sh ${DEFAULT_DIR}/palo/env.sh
COPY palo/docker/CentOS-Base.repo /etc/yum.repos.d/CentOS-Base.repo
# RUN cp ${DEFAULT_DIR}/palo/docker/CentOS-Base.repo /etc/yum.repos.d
RUN cd ${DEFAULT_DIR}/palo && /bin/bash thirdparty/build-thirdparty.mt.sh \
    && rm -rf ${DEFAULT_DIR}/palo
COPY jdk-12 /usr/local/java12
# use sankuai source for yum install
RUN groupadd -g 500 sankuai
RUN useradd -u 500 -g sankuai sankuai
RUN chown -R sankuai /var/local/ldb-toolchain
CMD ["/bin/bash"]

# ----begin: added from weihuanxiang----
RUN  yum -y install python3 \
  && python3 -m pip install --upgrade pip==21.3.1 --trusted-host pypi.sankuai.com -i http://pypi.sankuai.com/simple/ \
  && pip3 install --upgrade unity-client bci-cli -i http://pypi.sankuai.com/simple --trusted-host pypi.sankuai.com \
  && wget https://s3plus.sankuai.com/v1/mss_aee445ae7aa0438c82eacce6e3f6cb2c/beeget/jdk-8u112-linux-x64.tar.gz -O /usr/local/jdk-8u112-linux-x64.tar.gz \
  && tar xf /usr/local/jdk-8u112-linux-x64.tar.gz -C /usr/local && rm -rf /usr/local/jdk-8u112-linux-x64.tar.gz \
  && wget https://s3plus.sankuai.com/v1/mss_aee445ae7aa0438c82eacce6e3f6cb2c/beeget/gradle-5.6.2-bin.zip -O /opt/gradle-5.6.2-bin.zip \
  && unzip /opt/gradle-5.6.2-bin.zip -d /opt/ && chown -R root:root /opt/gradle-5.6.2 && rm -rf /opt/gradle-5.6.2-bin.zip \
  && wget https://s3plus.sankuai.com/v1/mss_aee445ae7aa0438c82eacce6e3f6cb2c/beeget/apache-groovy-sdk-2.5.6.zip -O /usr/local/apache-groovy-sdk-2.5.6.zip \
  && unzip /usr/local/apache-groovy-sdk-2.5.6.zip -d /usr/local && rm -rf /usr/local/apache-groovy-sdk-2.5.6.zip \
  && wget https://s3plus.sankuai.com/v1/mss_aee445ae7aa0438c82eacce6e3f6cb2c/beeget/apache-groovy-sdk-4.0.1.zip -O /usr/local/apache-groovy-sdk-4.0.1.zip \
  && unzip /usr/local/apache-groovy-sdk-4.0.1.zip -d /usr/local && rm -rf /usr/local/apache-groovy-sdk-4.0.1.zip \
  && mkdir -p /root/.m2 && wget -P /root/.m2/ http://s3plus.vip.sankuai.com/v1/mss_8b80ba092e4145088a62d10f25a14f36/resource/maven/local/settings.xml \
  && wget https://s3plus.sankuai.com/v1/mss_aee445ae7aa0438c82eacce6e3f6cb2c/beeget/slave_ssh_common_config_20211117.tar.gz -O /tmp/slave_ssh_common_config_20211117.tar.gz \
  && mkdir /root/.ssh && tar -zxvf /tmp/slave_ssh_common_config_20211117.tar.gz -C /root/.ssh \
  && chown root /root/.ssh/config /root/.ssh/git_lxsdk_autotest_rsa /root/.ssh/git_lxsdk_autotest_rsa.pub \
  && ssh-keyscan -H git.sankuai.com >> ~/.ssh/known_hosts

# ADD bash_profile /root/.bash_profile

#设置时区
RUN /bin/cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo 'Asia/Shanghai' > /etc/timezone

WORKDIR /opt

ENV LC_ALL="en_US.UTF-8" LANG="zh_CN.GBK"
ENV JAVA_HOME="/usr/local/jdk1.8.0_112"
ENV GROOVY_HOME="/usr/local/groovy-2.5.6"
ENV GRADLE_HOME="/opt/gradle-5.6.2"
ENV PATH=$JAVA_HOME/bin:$GRADLE_HOME/bin:$GROOVY_HOME/bin:$PATH
# ----end: added from weihuanxiang----
