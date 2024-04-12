#!/bin/bash

WORK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# start docker and pull image
systemctl start docker
docker pull apache/doris:build-env-ldb-toolchain-latest

# clean running docker avoid conflict
docker rm -f $(docker ps -a -q)
docker run -it -v ${WORK_DIR}/.m2:/root/.m2 -v ${WORK_DIR}/:/root  --name TCHouse-D -d apache/doris:build-env-ldb-toolchain-latest
docker exec -it TCHouse-D /bin/bash
