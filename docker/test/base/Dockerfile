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

# docker build -t apachedoris/doris-dev:test-base .

FROM centos:7 AS tester-base

# install dependencies
RUN yum makecache && yum -y update && yum groupinstall "Development Tools" -y && \
    yum install openssl-devel libffi-devel bzip2-devel -y && \
    yum install wget -y && \
    yum install -y java-11-openjdk-devel && \
    yum install -y python3

RUN alternatives --set java java-11-openjdk.x86_64 && alternatives --set javac java-11-openjdk.x86_64

RUN python3 -m pip install PyMySQL
RUN python3 -m pip install requests
COPY doris-client.py /root/
RUN echo "alias doris-client='python3 /root/doris-client.py'" >> /root/.bashrc

ENV JAVA_HOME="/usr/lib/jvm/java-11"