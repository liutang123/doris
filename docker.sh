systemctl start docker
docker pull apache/doris:build-env-for-1.2
docker rm -f $(docker ps -a -q)
#docker run -it -v /data/doris-1.x/.m2:/root/.m2 -v /data/doris-1.x/:/data/doris-1.x  --name doris-1.x -d apache/doris:build-env-ldb-toolchain-latest
docker run -it -v /data/doris-1.x/.m2:/root/.m2 -v /data/doris-1.x/:/data/doris-1.x  --name doris-1.x -d apache/doris:build-env-for-1.2
docker exec -it doris-1.x /bin/bash
