#!/bin/bash

WORK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# check if or not has debug commit need to revert
ignored_commits=(
  "78d99c6fdfe41e3fc0979c0e1feead8ab6a3e217"
  "9579634eac68e6f65099a1f4f78687f36eff3a23"
  "4de21d604afe0779b34de8586e3fbfd618551054"
)

commits_to_revert=()
commits=$(git log --grep="^\[Debug" --pretty=format:"%H")
if [ ! -z "$commits" ]; then
  for commit in $commits; do
    if [[ " ${ignored_commits[@]} " =~ " ${commit} " ]]; then
      #echo "Commit $commit is in the ignore list. Skipping..."
      continue
    fi
    if git log --grep="This reverts commit $commit" --format=%B | grep -q "This reverts commit $commit"; then
      #echo "Commit $commit has already been reverted. Skipping..."
      continue
    fi
    #echo "Need to revert commit $commit..."
    commits_to_revert+=("$commit")
  done
fi

if [ ${#commits_to_revert[@]} -gt 0 ]; then
  echo "The following commits need to be reverted:"
  for commit in "${commits_to_revert[@]}"; do
    echo "$commit"
  done
  exit 1
fi

# start docker and pull image
systemctl start docker
docker pull apache/doris:build-env-ldb-toolchain-latest

# clean running docker avoid conflict
#docker rm -f $(docker ps -a -q)
CONTAINER_IDS=$(docker ps -a -q)
if [ -n "$CONTAINER_IDS" ]; then
  docker rm -f $CONTAINER_IDS
fi

# Download maven repo for first time
ARCHIVE_NAME="cdw-doris-2.1-maven-repo.tar.bz2"
DOWNLOAD_URL="https://cdwch-cos-apps-gz-1305504398.cos.ap-guangzhou.myqcloud.com/doris/$ARCHIVE_NAME"
if [ -d ".m2" ]; then
    echo ".m2 directory already exists."
else
    echo ".m2 directory does not exist. Downloading..."
    wget $DOWNLOAD_URL -O $ARCHIVE_NAME
    if [ $? -ne 0 ]; then
        echo "Error: Failed to download $ARCHIVE_NAME from $DOWNLOAD_URL"
        echo "Please check the URL and your network connection."
    else
        echo "Download completed. Extracting..."
        tar -xjf $ARCHIVE_NAME
        if [ $? -ne 0 ]; then
            echo "Error: Failed to extract $ARCHIVE_NAME"
            echo "Please check the archive file and try again."
        else
            echo "Extraction completed. .m2 directory is now available."
            rm -f $ARCHIVE_NAME
        fi
    fi
fi

# build
docker run -v ${WORK_DIR}/.m2:/root/.m2 -v ${WORK_DIR}/:/root  --name doris-2.x apache/doris:build-env-ldb-toolchain-latest /bin/bash -c /root/build_all.sh
if [ $? -ne 0 ]; then
  echo "[ERROR] failed to build with docker."
  exit 1
fi
echo "[INFO] success to build with docker."
