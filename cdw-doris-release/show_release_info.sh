#!/bin/bash

#set -x

################################################################################################################################
#
# define region: you can change here
#
doris_dir_name="doris"
coscli_cmd="/data/coscli-linux"
cos_backup_subdir="cdw_doris_backup"
log_file="log_package_and_deploy_to_cos.log"
bucket="derenli-1301087413"
region="guangzhou"
doris_release_package_dir="doris_release_package"
doris_be_debug_package_dir="doris_be_debug_package"
upgrade_script_file="upgrade_doris_from_cos.sh"

cos_bucket="cos://${bucket}"
download_cos_url="https://${bucket}.cos.ap-${region}.myqcloud.com/${doris_release_package_dir}"
download_debug_doris_be_cos_url="https://${bucket}.cos.ap-${region}.myqcloud.com/${doris_be_debug_package_dir}"
#
################################################################################################################################

curdir=$(dirname "$0")
curdir=$(
  cd "$curdir"
  pwd
)
workDir=$curdir
logFile=${workDir}/${log_file}

log() {
  echo "$@" >> ${logFile}
  echo "$@"
}

init() {
  local doris_dir="${workDir}/${doris_dir_name}"
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
  doris_tar_sha="${doris_tar}.sha512"
  doris_be_without_strip_tar="${doris_version_string}-doris_be.tar.gz"
  doris_tar_sha_file_path="${workDir}/${doris_tar_sha}"

  if [ ! -f "${workDir}/${doris_tar}" ]; then
    log "[ERROR] ${workDir}/${doris_tar} is not exist, please run deploy.sh first."
    exit -1
  fi

  if [ ! -f "${workDir}/${doris_be_without_strip_tar}" ]; then
    log "[ERROR] ${workDir}/${doris_be_without_strip_tar} is not exist, please run deploy.sh first."
    exit -1
  fi

  log "[INFO] doris version is ${doris_version_string}"
  log "[INFO] doris tar package is ${doris_tar}"
  log "[INFO] sha512 file for doris tar package is ${doris_tar_sha}"
  log "[INFO] upgrade script file for doris is ${upgrade_script_file}"
  log "[INFO] cos bucket is ${cos_bucket}"
}

show_release_info() {
  echo ""
  echo "****************** release infomation ******************"
  echo "Version:"
  echo $(${workDir}/doris/lib/be/doris_be --version)
  echo "MD5 info:"
  cd ${workDir}
  md5sum "${doris_tar}"
  md5sum "doris/lib/be/doris_be"
  md5sum "doris/lib/fe/doris-fe.jar"

  echo "SHA info:"
  local str=$(cat ${workDir}/${doris_tar_sha})
  local len=${#str}
  local max_len=60
  local strtail=$((len%${max_len}))
  local line=$((len/${max_len}))
  local sPos=0
  for ((n=1;n<=$line;n++))
  do
    echo ${str:sPos:${max_len}}
    sPos=$((sPos+${max_len}))
  done
  if [ $strtail -ne 0 ]; then
    echo ${str:sPos:strtail}
  fi
  echo "Release Date: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "Download URL:"
  echo "${download_cos_url}/${doris_version_string}.tar.gz"
  echo "Debug doris_be URL:"
  echo "${download_debug_doris_be_cos_url}/${doris_be_without_strip_tar}"
  echo "********************************************************"
  echo ""
}

show_git_log_message() {
  SKIP_LINE_NUM=0
  git log -5 --skip=$SKIP_LINE_NUM --date=format:'[%Y-%m-%d %H:%M:%S]' --pretty=format:"%ad [%an] %s"
  echo "..."
  SKIP_LINE_NUM=$(git log --oneline | egrep -c -w "^[^[:space:]]+\s+\[Tencent\]")
  git log -10 --skip=$SKIP_LINE_NUM --date=format:'[%Y-%m-%d %H:%M:%S]' --pretty=format:"%ad [%an] %s"
}

init $@
show_release_info || true
show_git_log_message || true
