#!/bin/bash

WORK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# start docker and pull image
systemctl start docker
docker pull apache/doris:build-env-for-1.2

# clean running docker avoid conflict
docker rm -f $(docker ps -a -q)

# build
docker run -it -v ${WORK_DIR}/doris-1.2.m2:/root/.m2 -v ${WORK_DIR}:${WORK_DIR}  --name doris-1.2 -d apache/doris:build-env-for-1.2
docker exec -it doris-1.2 /bin/bash
