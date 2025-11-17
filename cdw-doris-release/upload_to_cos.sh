#!/bin/bash

#set -x

################################################################################################################################
#
# define region: you can change here
#
DORIS_DIR_NAME="doris"
# default config file is $HOME/.cos.yaml
COSCLI_CMD="/data/coscli-linux" 
COSCLI_CMD_CONF="$HOME/.release_cos.yaml"
LOG_FILE="log_package_and_deploy_to_cos.log"
ADDRESSES=(
  "cdwch-cos-apps-bj-1305504398.cos.ap-beijing.myqcloud.com"
  "cdwch-cos-apps-gz-1305504398.cos.ap-guangzhou.myqcloud.com"
  "cdwch-cos-apps-sh-1305504398.cos.ap-shanghai.myqcloud.com"
  "cdwch-cos-apps-nj-1305504398.cos.ap-nanjing.myqcloud.com"
  "cdwch-cos-apps-hk-1305504398.cos.ap-hongkong.myqcloud.com"
  "cdwch-cos-apps-cq-1305504398.cos.ap-chongqing.myqcloud.com"
  "cdwch-cos-apps-sp-1305504398.cos.ap-singapore.myqcloud.com"
  "cdwch-cos-apps-cd-1305504398.cos.ap-chengdu.myqcloud.com"
  "cdwch-apps-shadc-1305504398.cos.ap-shanghai-adc.myqcloud.com"
  "cdwch-cos-apps-shjr-1305504398.cos.ap-shanghai-fsi.myqcloud.com"
  "cdwch-cos-apps-us-1305504398.cos.na-siliconvalley.myqcloud.com"
  "cdwch-cos-apps-th-1305504398.cos.ap-bangkok.myqcloud.com"
  "cdwch-cos-apps-use-1305504398.cos.na-ashburn.myqcloud.com"
  "cdwch-cos-apps-jp-1305504398.cos.ap-tokyo.myqcloud.com"
  "cdwch-cos-apps-jkt-1305504398.cos.ap-jakarta.myqcloud.com"
  "cdwch-cos-apps-szjr-1305504398.cos.ap-shenzhen-fsi.myqcloud.com"
  "cdwch-cos-apps-kr-1305504398.cos.ap-seoul.myqcloud.com"
)
#
################################################################################################################################

curdir=$(dirname "$0")
curdir=$(
  cd "$curdir"
  pwd
)
workDir=$curdir
logFile=${workDir}/${LOG_FILE}

log() {
  echo "$@" >> ${logFile}
  echo "$@"
}

usage(){
  echo "USAGE: $0 region"
  echo " e.g.: \"$0 bj"
  echo " e.g.: \"$0 gz"
  echo " e.g.: \"$0 sh"
  echo " e.g.: \"$0 nj"
  echo " e.g.: \"$0 hk"
  echo " e.g.: \"$0 cq"
  echo " e.g.: \"$0 sg"
  echo " e.g.: \"$0 cd"
}

match_cos_region_address() {
  local keyword="$1"
  if [[ -z "$keyword" ]]; then
    echo ""
    return 0
  fi
  keyword=$(echo "$keyword" | tr '[:upper:]' '[:lower:]')
  for addr in "${ADDRESSES[@]}"; do
    addr_lower=$(echo "$addr" | tr '[:upper:]' '[:lower:]')
    if [[ "$addr_lower" =~ "apps-${keyword}-" ]]; then
      echo "$addr"
      return 0
    fi
  done
  echo ""
  return 0
}

init() {
  # You can specify the target release region for the upload through the parameter, default for chongqing
  if [ $# -ne 0 ] && [ $# -ne 1 ] ; then
    usage $@
    exit 1;
  fi

  REGION="cq"
  if [ $# -eq 1 ] ; then
    REGION=$1
  fi
  local cos_addr=$(match_cos_region_address "$REGION")
  if [[ -z "$cos_addr" ]]; then
    log "[ERROR] Invalid region code: $region"
    usage $@
    exit 1
  fi
  log "[INFO] cos address URL is ${cos_addr}"

  local doris_dir="${workDir}/${DORIS_DIR_NAME}"
  if [ ! -d "${doris_dir}" ]; then
    log "[ERROR] ${doris_dir} is not exist, please build and deploy first."
    exit -1
  fi

  local version_file="${doris_dir}/version.txt"
  if [[ ! -f ${version_file} ]]; then
    log "[ERROR] failed to find version.txt in ${workDir}"
    exit 1
  fi

  # now init global variables
  read -r dump1 dump2 doris_version_string < $version_file
  doris_tar="${doris_version_string}.tar.gz"
  doris_be_without_strip_tar="${doris_version_string}-doris_be.tar.gz"

  log "[INFO] doris version is ${doris_version_string}"
  log "[INFO] doris tar package is ${doris_tar}"

  if [ ! -f "${workDir}/${doris_tar}" ]; then
    log "[ERROR] ${workDir}/${doris_tar} is not exist, please run deploy.sh first."
    exit -1
  fi

  if [ ! -f "${workDir}/${doris_be_without_strip_tar}" ]; then
    log "[ERROR] ${workDir}/${doris_be_without_strip_tar} is not exist, please run deploy.sh first."
    exit -1
  fi

  local bucket="cdwch-cos-apps-${REGION}-1305504398"
  local cos_bucket="cos://${bucket}" 
  local cos_doris_dir=$(get_release_cos_bucket_subdir)
  COS_RELEASE_URL="${cos_bucket}/${cos_doris_dir}"
  DOWNLOAD_COS_URL="https://${cos_addr}/${cos_doris_dir}/${doris_tar}"
  DOWNLOAD_DEBUG_DORIS_BE_COS_URL="https://${cos_addr}/${cos_doris_dir}/${doris_be_without_strip_tar}"
}

get_release_cos_bucket_subdir() {
  local version_string=$(echo $doris_tar | egrep -o "[0-9]\.[0-9]+" | head -1)
  # decide the dir according to new version
  local cos_subdir=""
  case ${version_string} in
    0.15)
        cos_subdir="0.15.0"
        ;;
    1.0)
        cos_subdir="1.0.1"
        ;;
    1.1)
        cos_subdir="1.1.0"
        ;;
    1.2)
        cos_subdir="1.2.0"
        ;;
    2.0)
        cos_subdir="2.0"
        ;;
    *) # from 2.0, cos_subdir is the same as major version
        cos_subdir=${version_string}
        ;;
  esac
  echo "doris/${cos_subdir}"
}

package_and_deploy_to_cos() {
  log "[INFO] start to deploy to cos."

  local doris_tar_path="${workDir}/${doris_tar}"
  if [ ! -f ${doris_tar_path} ]; then
    log "[ERROR] ${doris_tar_path} is not exist, please build and deploy first."
    exit 1
  fi

  local doris_be_without_strip_tar_path="${workDir}/${doris_be_without_strip_tar}"
  if [ ! -f ${doris_be_without_strip_tar_path} ]; then
    log "[ERROR] ${doris_be_without_strip_tar_path} is not exist, please build and deploy first."
    exit 1
  fi

  # upload to cos
  log "[INFO] start to upload the tar package ${doris_tar_path} to cos bucket, it need to a few minutes..."
  if [ ! -e ${COSCLI_CMD} ]; then
    log "[ERROR] ${COSCLI_CMD} is not exist, please download to this path first."
    exit 1
  fi

  if [ ! -f ${COSCLI_CMD_CONF} ]; then
    log "[ERROR] ${COSCLI_CMD_CONF} is not exist, please add and edit this conf file."
    exit 1
  fi

  ${COSCLI_CMD} -c ${COSCLI_CMD_CONF} cp ${doris_tar_path} ${COS_RELEASE_URL}/${doris_tar}
  if [ $? -ne 0 ]; then
    log "[ERROR] upload ${doris_tar_path} to ${COS_RELEASE_URL}/${doris_tar} failed!"
    exit 1
  fi

  if [ "${REGION}" == "cq" ]; then
    # upload doris_be without strip tar file
    ${COSCLI_CMD} -c ${COSCLI_CMD_CONF} cp ${doris_be_without_strip_tar_path} ${COS_RELEASE_URL}/${doris_be_without_strip_tar}
    if [ $? -ne 0 ]; then
      log "[ERROR] upload ${doris_be_without_strip_tar_path} to ${COS_RELEASE_URL}/${doris_be_without_strip_tar} failed!"
      exit 1
    fi
    log "[INFO] upload ${doris_be_without_strip_tar_path} to ${COS_RELEASE_URL}/${doris_be_without_strip_tar} ok"
  fi

  log "[INFO] success to release doris"
}

show_release_info() {
  echo ""
  echo "****************** release infomation ******************"
  echo "Version:"
  echo ${doris_version_string}
  echo "MD5 info:"
  cd ${workDir}
  md5sum "${doris_tar}"
  md5sum "doris/lib/be/doris_be"
  md5sum "doris/lib/fe/doris-fe.jar"

  echo "Release Date: $(date '+%Y-%m-%d %H:%M:%S')" 
  echo "Download URL:"
  echo "${DOWNLOAD_COS_URL}"
  echo "Debug doris_be URL:"
  echo "${DOWNLOAD_DEBUG_DORIS_BE_COS_URL}"
  echo "********************************************************"
  echo ""
}

show_git_log_message() {
  SKIP_LINE_NUM=0
  git log -5 --skip=$SKIP_LINE_NUM --date=format:'[%Y-%m-%d %H:%M:%S]' --pretty=format:"%ad [%an] %s"
  echo "..."
  SKIP_LINE_NUM=$(git log --oneline | egrep -c "^[^[:space:]]+\s+\[Tencent\]")
  git log -10 --skip=$SKIP_LINE_NUM --date=format:'[%Y-%m-%d %H:%M:%S]' --pretty=format:"%ad [%an] %s"
}

create_and_push_new_branch() {
  local current_version=$(git branch --show-current)
  if git tag | grep -q "^${doris_version_string}$"; then
    echo "Tag '${doris_version_string}' already exists."
  else
    git tag -a ${doris_version_string} -m "${doris_version_string}"
    git push tencent_origin ${doris_version_string}
    git checkout ${current_version}
  fi
}

init $@
package_and_deploy_to_cos
show_release_info || true
show_git_log_message || true
create_and_push_new_branch || true
