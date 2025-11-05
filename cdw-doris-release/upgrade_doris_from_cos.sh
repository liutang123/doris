#!/bin/bash

#set -o errexit
#trap 'exit_on_error "unexpected error has occured"' ERR

################################################################################################################################
#
# define region: you can change or add new region here
#
WORK_DIR_PREFIX="/data/cdw/upgrade_packages_dir"
# List of known bucket ADDRESSES for region validation
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

# double write log info to file and term
log() {
  if [[ -n "${LOG_FILE}" ]]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') $@" | tee -a "$LOG_FILE"
  else
    echo "$(date '+%Y-%m-%d %H:%M:%S') $@"
  fi
}

exit_on_error() {
  log "[ERROR] $@" 
  exit 1
}

# rollback new add dirs
rollback_new_add_paths() {
  log "[INFO] start to rollback new add files or dirs..."
  if [ ! -f ${NEW_ADD_PATHS_LIST} ]; then
    log "[INFO] ${NEW_ADD_PATHS_LIST} file is not exist!"
    return 0
  fi

  local new_add_paths=($(cat "${NEW_ADD_PATHS_LIST}" | sort -u))
  for new_add_path in "${new_add_paths[@]}"; do
    if [[ -f "$new_add_path" || -d "$new_add_path" ]]; then
      rm -fr ${new_add_path} || { log "[ERROR] rm -fr ${new_add_path} failed"; return 1; }
      log "[INFO] remove dir ${new_add_path}"
    else
      log "[WARN] ${new_add_path} is not exist, it maybe run rollback for more times!"
    fi
  done

  log "[INFO] rollback new add files and dirs succuesfullly"
  return 0
}

# rollback upgrade dirs
rollback_upgrade_paths() {
  log "[INFO] start to rollback updated files or dirs..."
  if [ ! -f ${BACKUP_UPGRADE_PATH_PAIR_LIST} ]; then
    log "[INFO] ${BACKUP_UPGRADE_PATH_PAIR_LIST} file is not exist!"
    return 0
  fi

  local upgrade_path_pair_list=($(cat "${BACKUP_UPGRADE_PATH_PAIR_LIST}"))
  for path_pair in "${upgrade_path_pair_list[@]}"; do
    local backup_path=$(echo ${path_pair} | awk -F ',' '{print $1}')
    local upgrade_path=$(echo ${path_pair} | awk -F ',' '{print $2}')

    if [ "${backup_path}" == "" -o "${upgrade_path}" == "" ]; then
      log "[WARN] either backup dir and upgrade dir cannot empty"
      return 1
    fi

    if [[ ! -f "$backup_path" && ! -d "$backup_path" ]]; then
      log "[WARN] ${backup_path} is not exists!"
      continue
    fi

    if [ -d ${upgrade_path} ]; then
      log "[INFO] ${upgrade_path} is exist, it will be remove!"
      rm -fr "${upgrade_path}" || { log "[ERROR] rm -fr ${upgrade_path} failed"; return 1; }
    fi

    cp -a "${backup_path}" "${upgrade_path}" || { log "[ERROR] copy ${backup_path} to ${upgrade_path} failed"; return 1; }
    log "[INFO] restore dir from ${backup_path} to ${upgrade_path}"
  done

  log "[INFO] rollback updated files and dirs succuesfullly"
  return 0
}

# rollback automatically when the error occurs in upgrading
error_on_rollback() {
  log "[ERROR] $@" 
  log "[INFO] Rollback..." 
  rollback_new_add_paths
  rollback_upgrade_paths
  exit 1
}

# just for those upgarding from 1.1 to 1.2 and need to swith user from root to doris
monitor_json_fe_doris=$(cat <<- 'EOF'
[{"processName":"org.apache.doris.PaloFe","startCmd":{"cmd":"sh","timeout":3600,"arguments":["/usr/local/service/doris/bin/start_fe.sh","--daemon"],"environments":null},"user":"doris","group":"doris","port":""},{"processName":"org.apache.doris.broker.hdfs.BrokerBootstrap","startCmd":{"cmd":"sh","timeout":3600,"arguments":["/usr/local/service/doris/bin/start_broker.sh","--daemon"],"environments":null},"user":"doris","group":"doris","port":""}]
EOF
)

monitor_json_be_doris=$(cat <<- 'EOF'
[{"processName":"doris_be","startCmd":{"cmd":"sh","timeout":3600,"arguments":["/usr/local/service/doris/bin/start_be.sh","--daemon"],"environments":null},"user":"doris","group":"doris","port":""},{"processName":"org.apache.doris.broker.hdfs.BrokerBootstrap","startCmd":{"cmd":"sh","timeout":3600,"arguments":["/usr/local/service/doris/bin/start_broker.sh","--daemon"],"environments":null},"user":"doris","group":"doris","port":""}]
EOF
)

# for 1.1 to 1.2, sometimes need to update monitor json
update_monitor_json() {
  local monitor_json="/usr/local/service/cdwch/monitor.json"
  local monitor_json_bak="${monitor_json}.bak"
  local monitor_json_old="${monitor_json}.old"

  local need_to_fix=true
  local node_type=""
  if [ -f ${monitor_json} ]; then
    local actual_json=`echo $(cat "${monitor_json}")`
    cat "${monitor_json}" | grep start_be > /dev/null
    if [ $? -eq 0 ]; then # for BE
      node_type="BE"
      if [ "${monitor_json_be_doris}" == "${actual_json}" ]; then
        need_to_fix=false
      fi
    fi
      
    cat "${monitor_json}" | grep PaloFe > /dev/null
    if [ $? -eq 0 ]; then
      if [ "${node_type}" == "BE" ]; then
        log "[WARN] there are some errors in ${monitor_json}($(cat ${monitor_json}))"
        return 1
      fi
      node_type="FE"
      if [ "${monitor_json_fe_doris}" == "${actual_json}" ]; then
        need_to_fix=false
      fi
    fi
  fi
  if [ "${need_to_fix}" = false ]; then
    log "[INFO] no need to fix monitor.json"
    return 0
  fi

  # check and make sure monitor bak file exist
  if [ ! -f ${monitor_json_bak} ]; then
    if [ ! -f ${monitor_json} ]; then
      log "[WARN] Update monitor json failed (Both ${monitor_json} and ${monitor_json_bak} are not exist!)"
      return 1
    fi
    mv "${monitor_json}" "${monitor_json_bak}"
    if [ $? -ne 0 ]; then
      log "[WARN] Update monitor json failed (move ${monitor_json} to ${monitor_json_bak} failed!)"
      return 1
    fi
    log "[INFO] move ${monitor_json} to ${monitor_json_bak}"
  fi

  # backup old monitor
  mv "${monitor_json_bak}" "${monitor_json_old}" || { log "[ERROR] Move ${monitor_json_bak} to ${monitor_json_old} failed"; return 1; }
  log "[INFO] move ${monitor_json_bak} to ${monitor_json_old}"

  # create new monitor files according to old
  cat "${monitor_json_old}" | grep start_be > /dev/null
  if [ $? -eq 0 ]; then
    echo ${monitor_json_be_doris} > ${monitor_json_bak} || { log "[ERROR] Create monitor json failed!"; return 1; }
    if [ "${node_type}" == "FE" ]; then
      log "[WARN] there are some errors in ${monitor_json_old}($(cat ${monitor_json_old}))"
      return 1
    fi
    node_type="BE"
    log "[INFO] create new ${monitor_json_bak} for ${node_type} node"
  else 
    cat "${monitor_json_old}" | grep PaloFe > /dev/null
    if [ $? -eq 0 ]; then
      echo ${monitor_json_fe_doris} > ${monitor_json_bak}
      if [ "${node_type}" == "BE" ]; then
        log "[WARN] there are some errors in ${monitor_json_old}($(cat ${monitor_json_old}))"
        return 1
      fi
      node_type="FE"
      log "[INFO] create new ${monitor_json_bak} for ${node_type} node"
    else
      log "[WARN] there are some errors in ${monitor_json_old}($(cat ${monitor_json_old}))"
      log "[WARN] create new ${monitor_json_bak} failed!"
      return 1
    fi
  fi
  
  if [ "$1" == "RESTORE" ]; then
    cp -f ${monitor_json_bak} ${monitor_json} || { log "[ERROR] cp -f ${monitor_json_bak} to ${monitor_json} failed!"; return 1; }
    log "[INFO] restore ${monitor_json_bak} to ${monitor_json}"
  fi

  return 0
}

# for 1.1 to 1.2, sometimes need to update agent version
update_agent() {
  local agent_dir="/usr/local/service/cdwch/cdwch-agent"
  if [ -d ${agent_dir} ]; then
    local agent_file="${agent_dir}/bin/cdwch-agent"
    if [ -f ${agent_file} ]; then
      local latest_md5="54583b128dc1acdddbd22e1c4d0839bf"
      local current_md5=$(md5sum ${agent_file} | awk '{print $1}')
      if [ $? -eq 0 ] && [ "${current_md5}" == "${latest_md5}" ]; then
        log "[INFO] current ${agent_file} is the same as the latest one."
        log "[INFO] no need to fix cdwch-agent"
        return 0
      else
        log "[INFO] current ${agent_file} is old(md5sum is ${current_md5}), need to update..."
      fi
    fi
    rm -fr ${agent_dir} || { log "[ERROR] rm -fr ${agent_dir} failed"; return 1; }
    log "[INFO] remove dir ${agent_dir}"
  else
    log "[WARN] ${agent_dir} is not exist!"
  fi

  # remove tar package of old agent
  local agent_tar="/usr/local/service/cdwch/cdwdoris-agent-1.0.0.tar.gz"
  if [ -f ${agent_tar} ]; then
    rm -f ${agent_tar} || { log "[ERROR] rm -f ${agent_tar} failed"; return 1; }
    log "[INFO] remove file ${agent_tar}"
  else
    log "[WARN] ${agent_tar} is not exist!"
  fi

  # kill cdwch-agent for starting new version
  pkill -9 cdwch-agent
  if [ $? -ne 0 ]; then
    log "[WARN] kill cdwch-agent process failed!"
  else
    log "[INFO] killed cdwdoris-agent process and it will start automaticlly."
  fi

  return 0
}

# for 1.1 to 1.2, sometimes need to change working dir owner
change_working_dir_owner() {
  local need_to_fix=false
  local fe_dir="/data/cdw/doris/fe"
  local be_dir="/data/cdw/doris/be"
  local broker_dir="/data/cdw/doris/broker"
  local work_dir="/usr/local/service/doris"
#  local dir_list=(${fe_dir}/pid ${be_dir}/pid ${broker_dir}/pid ${fe_dir}/meta ${fe_dir}/temp_dir 
#                  ${be_dir}/storage ${be_dir}/lib/small_file ${be_dir}/var/pull_load
#                  ${fe_dir}/log ${be_dir}/log ${broker_dir}/log)
###################################################################
# Here check and change high level than start script do, 
# because sometime perhaps we create new file ownered by 
# root exceptionly in these dir, that should not stoping starting.
###################################################################
  local dir_list=(${fe_dir} ${be_dir} ${broker_dir} ${work_dir})
  for mydir in ${dir_list[*]}; do
    if [ ! -d ${mydir} ]; then
      log "[INFO] ${mydir} is not exist!" 
      continue
    fi
    local result=$(find ${mydir} -user root)
    if [ $? -ne 0 -o "$result" != "" ]; then
      log "[WARN] ${mydir} is ownered by root, now change to doris"
      need_to_fix=true
      chown -R doris:doris ${mydir} || { log "[FATAL] change the owner of ${mydir} to doris:doris failed"; return 1; }
    else
      log "[INFO] check ${mydir} is OK."
    fi
  done
  if [ "${need_to_fix}" = false ]; then
    log "[INFO] no working dir are belong to root"
  fi

  return 0
}

# download new package
download_new_package() {
  local source_file=${NEW_VERSION_DORIS_TAR_PACKAGE}
  log "[INFO] start to prepare the doris ${source_file} package."

  # if exists in /data, move to SOURCE_DIR and return 0
  log "[INFO] try to find the doris ${source_file} package in /data."
  if [ -f "/data/${source_file}" ]; then
    log "[INFO] ${source_file} is exists in dir /data, move to ${SOURCE_DIR}/${source_file}..."
    mv "/data/${source_file}" "${SOURCE_DIR}/${source_file}" 
    if [ $? -eq 0 ]; then
      log "[INFO] success to move file ${source_file} from /data to ${SOURCE_DIR}!"
      return 0
    fi
  fi

  # download from cos
  log "[INFO] try to download ${source_file} from cos since it is not found in /data."
  local new_version=$NEW_MAJOR_VERSION
  local cos_subdir=""
  case ${new_version} in
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
        cos_subdir=${new_version}
        ;;
  esac
  cos_subdir="doris/${cos_subdir}"
  log "[INFO] start to wget from cos, and the url is ${COS_ADDR}/${cos_subdir}/${source_file}"
  wget -q ${COS_ADDR}/${cos_subdir}/${source_file} -P ${SOURCE_DIR}
  if [ $? -eq 0 ]; then
    log "[INFO] success to download the doris ${source_file} package from cos and saved to ${SOURCE_DIR}."
    return 0
  fi

  log "[ERROR] download file ${source_file} failed!"
  return 1
}

# decompress new package
decompress_new_package() {
  local untar_work_dir="${SOURCE_DIR}/"
  local upgrade_file_path="${untar_work_dir}/${NEW_VERSION_DORIS_TAR_PACKAGE}"

  log "[INFO] start to decompress doris package $upgrade_file_path ..."
  tar -zxf "${upgrade_file_path}" -C "${untar_work_dir}" || { log "[ERROR] unzip tar package ${upgrade_file_path} failed!"; return 1; }
  log "[INFO] success to decompress $upgrade_file_path to ${untar_work_dir}."
  return 0
}

# backup old doris dir
backup_old_doris_dir() {
  local dest_dir="${DEST_DIR}/doris"
  log "[INFO] start to backup old doris (${dest_dir})..."
  cp -a "${dest_dir}" "${BACKUP_DIR}" || { log "[ERROR] copy ${dest_dir} to ${BACKUP_DIR} failed." ; return 1; }
  echo "${BACKUP_DIR}/doris,${dest_dir}" >> ${BACKUP_UPGRADE_PATH_PAIR_LIST}
  log "[INFO] success to backup old doris ${dest_dir} to ${BACKUP_DIR}."
  return 0
}

# upgrade a specified component which specified by param
upgrade_a_specified_component() {
  if [[ $# -ne 1 ]]; then
    log "[ERROR] need to a param to specify the component"
    return 1
  fi

  local component=$(echo "$1" | tr '[:upper:]' '[:lower:]')
  if [[ -z "$component" ]]; then
    log "[ERROR] component param is null"
    return 1
  fi

  case "$component" in
    ms|fe|be|broker) log "[INFO] upgrade $component ...";;
    *) 
      log "[ERROR] invalid component $component ..."
      return 1
      ;;
  esac

  log "[INFO] start to upgrade the $component of doris..."

  local dest_dir="${DEST_DIR}/doris"
  local source_dir="${SOURCE_DIR}/doris"
  local backup_dir="${BACKUP_DIR}/doris"

  if [ ! -d "${source_dir}" ]; then
    log "[ERROR] download and decompress new doris first before you do upgrade!"
    return 1
  fi

  if [ ! -d "${backup_dir}" ]; then
    log "[ERROR] backup first before you do upgrade!"
    return 1
  fi

  log "[INFO] copy and replace the start script of $component of doris..."
  cp -f "${source_dir}/bin/start_${component}.sh" "${dest_dir}/bin/start_${component}.sh"
  if [ $? -ne 0 ]; then
    log "[ERROR] copy ${source_dir}/bin/start_${component}.sh to ${dest_dir}/bin/start_${component}.sh failed"
    return 1
  fi
  echo "${backup_dir}/bin/start_${component}.sh,${dest_dir}/bin/start_${component}.sh" >> ${BACKUP_UPGRADE_PATH_PAIR_LIST}

  log "[INFO] copy and replace the stop script of $component of doris..."
  cp -f "${source_dir}/bin/stop_${component}.sh" "${dest_dir}/bin/stop_${component}.sh"
  if [ $? -ne 0 ]; then
    log "[ERROR] copy ${source_dir}/bin/stop_${component}.sh to ${dest_dir}/bin/stop_${component}.sh failed"
    return 1
  fi
  echo "${backup_dir}/bin/stop_${component}.sh,${dest_dir}/bin/stop_${component}.sh" >> ${BACKUP_UPGRADE_PATH_PAIR_LIST}

  log "[INFO] remove the lib of $component of doris..."
  rm -fr "${dest_dir}/lib/${component}"
  if [ $? -ne 0 ]; then
    log "[ERROR] rm -fr ${dest_dir}/lib/${component} failed"
    return 1
  fi
  echo "${backup_dir}/lib/${component},${dest_dir}/lib/${component}" >> ${BACKUP_UPGRADE_PATH_PAIR_LIST}

  log "[INFO] copy the new lib of $component of doris..."
  cp -fr "${source_dir}/lib/${component}" "${dest_dir}/lib/"
  if [ $? -ne 0 ]; then
    log "[ERROR] copy ${source_dir}/lib/${component} to ${dest_dir}/lib failed"
    return 1
  fi

  log "[INFO] success to upgrade the $component of doris."
  return 0
}

# upgrade whole doris
upgrade_whole_doris() {
  log "[INFO] start to upgrade whole doris..."
  local dest_dir="${DEST_DIR}/doris"
  local source_dir="${SOURCE_DIR}/doris"
  local backup_dir="${BACKUP_DIR}/doris"

  if [ ! -d "${source_dir}" ]; then
    log "[ERROR] download and decompress new doris first before you do upgrade!"
    return 1
  fi

  if [ ! -d "${backup_dir}" ]; then
    log "[ERROR] backup first before you do upgrade!"
    return 1
  fi

  log "[INFO] remove the whole doris dir ${dest_dir}..."
  rm -fr "${dest_dir}" || { log "[ERROR] rm -fr ${dest_dir} failed"; return 1; }
  echo "${backup_dir},${dest_dir}" >> ${BACKUP_UPGRADE_PATH_PAIR_LIST}

  log "[INFO] copy the new whole doris dir (copy ${source_dir} to ${DEST_DIR})"
  cp -fr "${source_dir}" "${DEST_DIR}" || { log "[ERROR] copy ${source_dir}/lib/${component} to ${dest_dir}/lib failed"; return 1; }

  log "[INFO] success to upgrade the whole doris from $OLD_VERSION to $NEW_VERSION."
  return 0
}

# need to keep old configura and libs after replace and upgrade doris whole dir
keep_old_configure_and_libs() {
  # restore conf files
  local dest_dir="${DEST_DIR}/doris"
  local backup_dir="${BACKUP_DIR}/doris"
  log "[INFO] start to restore old conf and libs..."

  rm -fr ${dest_dir}/conf || { log "[ERROR] rm -fr ${dest_dir}/conf failed"; return 1; }
  cp -a "${backup_dir}/conf" "${dest_dir}" || { log "[ERROR] cp -a ${backup_dir}/conf ${dest_dir} failed"; return 1; }
  if [ -d "${dest_dir}/plugins/AuditLoader" ]; then
    log "[INFO] restore old conf of audit loader plugin."
    cp -f "${backup_dir}/plugins/AuditLoader/plugin.conf" "${dest_dir}/plugins/AuditLoader"
  fi

  # restore keytab file if exists
  if ls ${backup_dir}/*.keytab &> /dev/null; then
    log "[INFO] restore keytab files to ${dest_dir}."
    cp -f ${backup_dir}/*.keytab ${dest_dir} || { log "[ERROR] cp -f ${backup_dir}/*.keytab ${dest_dir} failed"; return 1; }
  fi

  # restore old jdbc drivers
  local old_jdbc_driver_dir="${backup_dir}/jdbc_drivers"
  local new_jdbc_driver_dir="${dest_dir}/jdbc_drivers"
  if [ -d "${old_jdbc_driver_dir}" ]; then
    log "[INFO] restore old jdbc_drivers."
    if [ ! -d "${new_jdbc_driver_dir}" ]; then
      mkdir -p ${new_jdbc_driver_dir} || { log "[ERROR] mkdir -p ${new_jdbc_driver_dir} failed"; return 1; }
    fi
    cp -f ${old_jdbc_driver_dir}/*.jar "${new_jdbc_driver_dir}" || { log "[ERROR] copy jars from ${old_jdbc_driver_dir} to ${new_jdbc_driver_dir} failed"; return 1; }
  fi

  # restore scripts for cloud manage controller 
  local new_scirpts_dir="${dest_dir}/bin"
  local apiserver_operation_sh="${backup_dir}/bin/apiserver_operation.sh"
  if [ -f "${apiserver_operation_sh}" ]; then
    log "[INFO] restore ${apiserver_operation_sh}."
    cp -f "${apiserver_operation_sh}" "${new_scirpts_dir}" || { log "[ERROR] cp -f ${apiserver_operation_sh} ${new_scirpts_dir} failed"; return 1; }
  fi
  local fe_monitor_sh="${backup_dir}/doris/bin/fe_monitor.sh"
  if [ -f "${fe_monitor_sh}" ]; then
    log "[INFO] restore ${fe_monitor_sh}."
    cp -f "${fe_monitor_sh}" "${new_scirpts_dir}" || { log "[ERROR] cp -f ${fe_monitor_sh} ${new_scirpts_dir} failed"; return 1; }
  fi

  log "[INFO] success to restore old conf and libs."
  return 0
}

# need to change from root to doris user group for upgrading from 1.2 and later
change_owner_from_root_to_doris() {
  local group="doris"
  local user="doris"
  log "[INFO] change owner from root to doris..."

  # 1. create group if not exists
  grep -E "^$group" /etc/group >& /dev/null
  if [ $? -ne 0 ]; then
    groupadd $group
    if [ $? -ne 0 ]; then
      log "[WARN] add group $group failed"
    else
      log "[INFO] Add group $group successfully!"
    fi
  fi

  # 2. create user if not exists
  grep -E "^$user" /etc/passwd >& /dev/null
  if [ $? -ne 0 ]; then
    useradd -g $group $user
    if [ $? -ne 0 ]; then
      log "[WARN] add user $user to $group failed"
    else
      log "[INFO] Add user $user successfully!"
    fi
  fi

  # 3. check and fix owners of working dir
  log "[INFO] check and fix owners of working dir"
  change_working_dir_owner || { log "[ERROR] check and fix owners of working dir failed"; return 1; }
  
  # 4. check and fix monitor.json
  log "[INFO] check and fix monitor.json"
  update_monitor_json || { log "[ERROR] check and fix monitor.json failed"; return 1; }

  # 5. check and fix cdwch-agent
  log "[INFO] check and fix cdwch-agent"
  update_agent || { log "[ERROR] check and fix cdwch-agent failed"; return 1; }

  log "[INFO] success to change owner from root to doris..."
  return 0
}

# need to check and prepare jdk17 for 3.0
check_install_jdk17() {
  local jdk_dest_dir="/usr/local"
  local jdk_dest_path="${jdk_dest_dir}/jdk17"
  log "[INFO] check if need to install jdk17..."

  if [ ! -d "${jdk_dest_path}" ]; then
    local jdk17_tar_file="TencentKona-17.0.13.b1-jdk_linux-x86_64.tar.gz"
    local file_url="${COS_ADDR}/doris/3.0/${jdk17_tar_file}"
    log "[INFO] ${jdk_dest_path} is not exists, it will start to wget from cos of $file_url"

    # download
    wget -q ${file_url} -P ${SOURCE_DIR} || { log "[ERROR] wget file ${jdk17_tar_file} failed!"; return 1; }
    log "[INFO] downloaded the file ${jdk17_tar_file} package."

    # untar
    log "[INFO] start to untar jdk17 package ..."
    tar -zxf "${SOURCE_DIR}/${jdk17_tar_file}" -C "${jdk_dest_dir}" || { log "[ERROR] unzip tar package ${jdk17_tar_file} failed"; return 1; }

    # install
    local jdk17_untar_file="${jdk_dest_dir}/TencentKona-17.0.13.b1"
    mv "${jdk17_untar_file}" "${jdk_dest_path}" || { log "[ERROR] Move ${jdk17_untar_file} to ${jdk_dest_path} failed"; return 1; }
    log "[INFO] Moved ${jdk17_untar_file} to ${jdk_dest_path}"

    log "[INFO] success to install jdk17"
  else
    log "[INFO] jdk17 is exist, no need to install."
  fi
  
  return 0
}

# check and fix the data dir of metaservice which is need to change the owner from root to doris.
check_and_fix_ms_dir() {
    local DIR="$1"
    local USER="doris"
    local GROUP="doris"

    log "[INFO] check and fix the data dir of metaservice to change the owner from root to doris..."
    if [ ! -d "$DIR" ]; then
	log "[WARN] $DIR is not a valid path"
        return 0
    fi

    local owner
    local group
    owner=$(stat -c %U "$DIR")
    group=$(stat -c %G "$DIR")

    if [ "$owner" != "$USER" ] || [ "$group" != "$GROUP" ]; then
        chown -R "$USER:$GROUP" "$DIR" || { log "[ERROR] chown failed"; return 1; }
    fi

    log "[INFO] success to change the owner of MS data dir from root to doris"
    return 0
}

# Validate region; return the address if found, or empty string otherwise
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

# Try to detect and return the current region automatically
detect_region() {
  # Example: get region from Tencent CVM metadata
  local meta_region
  meta_region=$(curl -s http://metadata.tencentyun.com/latest/meta-data/placement/region 2>/dev/null)
  if [[ -n "${meta_region}" ]]; then
    # meta_region may return like "ap-shanghai"
    for addr in "${ADDRESSES[@]}"; do
      if [[ "$addr" =~ \.(${meta_region})\.myqcloud\.com$ ]]; then
        if [[ "$addr" =~ apps-([a-z]+)-[0-9]+\.cos\.${meta_region}\.myqcloud\.com$ ]]; then
          echo "${BASH_REMATCH[1]}"
          return 0
        fi
      fi
    done
  fi
  echo ""
  return 0
}

# Validate version format, allow optional .tar.gz suffix
match_version() {
  local ver="$1"
  [[ "$ver" =~ ^tencent-cdw-doris-[0-9]+\.[0-9]+\.[0-9]+-[a-zA-Z0-9]+(-[a-zA-Z0-9]+){0,2}(\.tar\.gz)?$ ]]
}

# get old version
get_old_version() {
  local dest_dir="/usr/local/service/doris"

  # case 1: by version.txt
  local version_file="${dest_dir}/version.txt"
  local version_str=""
  if [ -f ${version_file} ]; then
    read -r dump1 dump2 version_str < $version_file
    if [[ $? -eq 0 && "$version_str" != "" ]]; then
      echo $version_str
      return 0
    fi
  fi

  # case 2: by doris_be --version
  local doris_be_bin="${dest_dir}/lib/be/doris_be"
  if [ ! -f ${doris_be_bin} ]; then
    #log "[INFO] doris_be is not exists, try to find palo_be in ${dest_dir}/lib/be."
    doris_be_bin="${dest_dir}/lib/be/palo_be"
  fi
  if [ -f ${doris_be_bin} ]; then
    version_str=$(${doris_be_bin} --version 2>/dev/null)
    if [[ $? -eq 0 && "$version_str" != "" ]]; then
      IFS=" (" read -r version _ <<< "$version_str"
      echo $version
      return 0
    fi
  fi

  # case 3: by start_be.sh --version
  version_str=$(sh /usr/local/service/doris/bin/start_be.sh --version)
  if [[ $? -eq 0 && "$version_str" != "" ]]; then
    IFS=" (" read -r version _ <<< "$version_str"
    echo $version
    return 0
  fi

  # can't decide the version
  echo ""
  return 1
}

usage() {
  echo "Usage: $0 [region|-r|rollback] <version> [--ms] [--fe] [--be] [--broker]"
  echo "Parameters can be in any order."
  echo "Examples:"
  echo "  $0 sh tencent-cdw-doris-3.0.8-xxxx-1234567 --ms --fe"
  echo "  $0 tencent-cdw-doris-1.1.5-rc02-ea64eeb --broker --be sh"
  echo "  $0 -r tencent-cdw-doris-1.2.10-6fe1462-2502272.tar.gz --broker"
  echo "  $0 rollback tencent-cdw-doris-1.1.5-rc02-ea64eeb.tar.gz --fe"
  exit 1
}

#############################################################################################################################
# Do upgrade or rollback
#############################################################################################################################

opt_ms=0
opt_fe=0
opt_be=0
opt_broker=0
opt_all=0

region=""
version=""
is_rollback=0

# Loop through arguments and classify them
while [[ $# -gt 0 ]]; do
  case "$1" in
    --ms)
      opt_ms=1
      shift
      ;;
    --fe)
      opt_fe=1
      shift
      ;;
    --be)
      opt_be=1
      shift
      ;;
    --broker)
      opt_broker=1
      shift
      ;;
    -r|rollback)
      is_rollback=1
      shift
      ;;
    -*)
      # Unknown flag
      echo "[ERROR] Unknown option $1"
      usage
      ;;
    *)
      # Non-flag argument, check if it's version or region
      if [[ -z "$version" ]] && match_version "$1" > /dev/null; then
        version="$1"
        shift
      elif [[ -z "$region" ]] && [[ -n "$(match_cos_region_address "$1")" ]]; then
        region="$1"
        shift
      else
        # Not recognized, may be input order issue
        echo "[ERROR] Unknown or invalid parameter: $1"
        usage
      fi
      ;;
  esac
done

# Version is mandatory
if [[ -z "$version" ]]; then
  echo "[ERROR] Version argument is required!"
  usage
fi

# trim the suffix if has
NEW_VERSION_DORIS_TAR_PACKAGE="${version}"
if [[ "${version}" != *.tar.gz ]]; then
  NEW_VERSION_DORIS_TAR_PACKAGE="${version}.tar.gz"
fi
upgrade_version_string=${NEW_VERSION_DORIS_TAR_PACKAGE%\.tar\.gz}

# create log file
date_str=$(date +"%Y%m%d_%H%M%S")
log_dir="${WORK_DIR_PREFIX}/${upgrade_version_string}"
if [[ $is_rollback -eq 0 ]]; then  # for upgrading
  LOG_FILE="${log_dir}/upgrade_${date_str}.log"
else
  LOG_FILE="${log_dir}/rollback_${date_str}.log"
fi
mkdir -p "$log_dir" || { echo "[ERROR] create log dir $log_dir failed"; exit 1; }
touch "$LOG_FILE" || { echo "[ERROR] create log file $LOG_FILE failed"; exit 1; }

# For upgrade mode, region is required. Try auto-detect if not specified.
COS_ADDR=""
if [[ $is_rollback -eq 0 ]]; then
  if [[ -z "$region" ]]; then
    region=$(detect_region)
    if [[ -z "$region" ]]; then
      log "[ERROR] Region argument is required in upgrade mode and could not be auto-detected!"
      usage
    else
      log "[INFO] Region auto-detected as: $region"
    fi
  fi
  COS_ADDR=$(match_cos_region_address "$region")
  if [[ -z "$COS_ADDR" ]]; then
    log "[ERROR] Invalid region code: $region"
    usage
  fi
fi

# For rollback mode, region is not required, but can be provided
if [[ $is_rollback -eq 1 && -n "$region" ]]; then
  COS_ADDR=$(match_cos_region_address "$region")
  if [[ -z "$COS_ADDR" ]]; then
    log "[ERROR] Invalid region code: $region"
    usage
  fi
fi

# add https prefix
COS_ADDR="https://$COS_ADDR"

# Print out final selections
if [[ $is_rollback -eq 1 ]]; then
  log "[INFO] Mode: Rollback"
else
  log "[INFO] Mode: Upgrade"
  log "[INFO] Region: $region"
  log "[INFO] COS address: $COS_ADDR"
fi
log "[INFO] Version: $version"
log "[INFO] Options:"
[[ $opt_ms -eq 1 ]] && log "  --ms"
[[ $opt_fe -eq 1 ]] && log "  --fe"
[[ $opt_be -eq 1 ]] && log "  --be"
[[ $opt_broker -eq 1 ]] && log "  --broker"

# No component options specified, defaulting to --ms --fe --be --broker
if [[ $opt_ms -eq 0 && $opt_fe -eq 0 && $opt_be -eq 0 && $opt_broker -eq 0 ]]; then
  opt_all=1
  log "[INFO] No component options specified, defaulting to --ms --fe --be --broker"
fi

# create source dir and backup dir
DEST_DIR="/usr/local/service"
SOURCE_DIR="${WORK_DIR_PREFIX}/${upgrade_version_string}/source"
BACKUP_DIR="${WORK_DIR_PREFIX}/${upgrade_version_string}/backup"
BACKUP_UPGRADE_PATH_PAIR_LIST="${BACKUP_DIR}/upgrade_path_pair_list.txt"
NEW_ADD_PATHS_LIST="${BACKUP_DIR}/new_add_paths_list.txt"

# figure out the old and new version
OLD_VERSION=$(get_old_version)
NEW_VERSION=$upgrade_version_string
OLD_MAJOR_VERSION=$(echo ${OLD_VERSION} | grep -E -o "[0-9]\.[0-9]+" | head -1)
NEW_MAJOR_VERSION=$(echo ${NEW_VERSION} | grep -E -o "[0-9]\.[0-9]+" | head -1)

[[ "$OLD_MAJOR_VERSION" =~ ^[0-9]+\.[0-9]+$ ]] || exit_on_error "Unrecognized old version: ${OLD_MAJOR_VERSION}"
[[ "$NEW_MAJOR_VERSION" =~ ^[0-9]+\.[0-9]+$ ]] || exit_on_error "Unrecognized new version: ${NEW_MAJOR_VERSION}"

# rollback or upgarde
if [[ $is_rollback -eq 0 ]]; then  # for upgrading
  log "[INFO] start to upgrade, the version from $OLD_VERSION to $NEW_VERSION..."

  # create dir
  log "[INFO] create dir ${SOURCE_DIR} and ${BACKUP_DIR}"
  if [ ! -d ${SOURCE_DIR} ]; then
    mkdir -p ${SOURCE_DIR} || exit_on_error "create dir ${SOURCE_DIR} failed"
  fi
  if [ ! -d ${BACKUP_DIR} ]; then
    mkdir -p ${BACKUP_DIR} || exit_on_error "create dir ${BACKUP_DIR} failed"
  fi

  # 1. Prepare the source packege of new doris version if it is not exist.
  if [ ! -f "${SOURCE_DIR}/${NEW_VERSION_DORIS_TAR_PACKAGE}" ]; then
    download_new_package || exit_on_error "prepare the source package failed"
  else
    log "[INFO] The target file ${NEW_VERSION_DORIS_TAR_PACKAGE} is already exists in ${SOURCE_DIR}!"
  fi

  # 2. Decompress the source packege of new doris version
  if [ ! -d "${SOURCE_DIR}/doris" ]; then
    decompress_new_package || exit_on_error "decompress the source package failed"
  else
    log "[INFO] It is been decompress in ${SOURCE_DIR}!"
  fi

  # 3. backup the old doris dir
  if [ ! -d "${BACKUP_DIR}/doris" ]; then
    backup_old_doris_dir || exit_on_error "backup old doris dir failed"
  else
    log "[INFO] It is been backup to ${BACKUP_DIR}/doris"
  fi

  # 4. upgrade doris according to the sepcified param
  if [[ $opt_all -ne 1 ]]; then
    if [[ $opt_ms -eq 1 ]]; then
      upgrade_a_specified_component "ms" || error_on_rollback "upgrade ms failed"
      # check and fix for ms dir is ownered by root...
      check_and_fix_ms_dir "/data/cdw/doris/ms" || error_on_rollback "check and fix ms dir failed"
    fi
    if [[ $opt_fe -eq 1 ]]; then
      upgrade_a_specified_component "fe" || error_on_rollback "upgrade fe failed"
    fi
    if [[ $opt_be -eq 1 ]]; then
      upgrade_a_specified_component "be" || error_on_rollback "upgrade be failed"
    fi
    if [[ $opt_broker -eq 1 ]]; then
      upgrade_a_specified_component "broker" || error_on_rollback "upgrade broker failed"
    fi
    
    log "[INFO] Upgrade component successfully!"
    exit 0
  fi

  # 5. upgrade the whole doris
  # Following is upgrade whole doris, that is opt_all must be 1
  upgrade_whole_doris || error_on_rollback "replace whole doris dir failed"

  # 6. restore and keep old configure and libs
  keep_old_configure_and_libs || error_on_rollback "restore and keep old configure and libs failed"

  # 7. For the upgarding from 1.1 to 1.2, need to do something...
  if [ "${NEW_MAJOR_VERSION}" == "1.2" ] && [ "${OLD_MAJOR_VERSION}" == "1.1" ]; then
    # need to change from root to doris user group for upgrading from 1.2 and later
    log "[INFO] need to change owner from root to doris due to upgrade from ${OLD_MAJOR_VERSION} to ${NEW_MAJOR_VERSION}"
    change_owner_from_root_to_doris || error_on_rollback "Change user from root to doris failed"
  fi

  # 8. need to check and prepare jdk17 for 3.0
  if [ "${NEW_MAJOR_VERSION}" == "3.0" ] && [ "${OLD_MAJOR_VERSION}" == "2.1" ]; then
    log "[INFO] need to check and install jdk17 due to upgrade from ${OLD_MAJOR_VERSION} to ${NEW_MAJOR_VERSION}"
    check_install_jdk17 || error_on_rollback "install jdk17 failed"
  fi

  # 9. check and fix for ms dir is ownered by root...
  check_and_fix_ms_dir "/data/cdw/doris/ms" || error_on_rollback "check and fix the owner of ms dir failed"
  log "[INFO] success to finish the upgrading, the version from $OLD_VERSION to $NEW_VERSION."
  
else # for rollback

  LOG_FILE="${WORK_DIR_PREFIX}/${upgrade_version_string}/rollback_${date_str}.log"
  log "[INFO] start to rollback..."

  if [ ! -d ${BACKUP_DIR} ]; then
    exit_on_error "backup dir(${BACKUP_DIR}) must be exist, please check the version number ${upgrade_version_string}"
  fi

  # remove new add dirs
  rollback_new_add_paths || exit_on_error "rollback new add dirs failed"

  # restore upgrade dirs
  rollback_upgrade_paths || exit_on_error "rollback upgrade dirs failed"

  log "[INFO] rollback successfully!!"
fi
