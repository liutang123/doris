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

# build
docker run -v ${WORK_DIR}/.m2:/root/.m2 -v ${WORK_DIR}/:/root  --name doris-2.x apache/doris:build-env-ldb-toolchain-latest /bin/bash -c /root/build_all.sh
if [ $? -ne 0 ]; then
  echo "[ERROR] failed to build with docker."
  exit 1
fi
echo "[INFO] success to build with docker."
