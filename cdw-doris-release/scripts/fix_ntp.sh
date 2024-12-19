#!/bin/bash

route_conf_file=/etc/sysconfig/network-scripts/route-eth1
ntp_conf_file=/etc/ntp.conf

restart_ntp_service() {
  echo "NTP server is restarting..."
  systemctl restart ntpd > /dev/null
  if [[ $? -ne 0 && -f "${ntp_conf_file}.${back_suffix}" ]]; then
    echo "Change ${ntp_conf_file} failed, roll back now"
    cp -f "${ntp_conf_file}.${back_suffix}" "${ntp_conf_file}" 
    systemctl restart ntpd > /dev/null
  fi
}

# Check if the ntpq command exists
if ! command -v ntpq &> /dev/null; then
    echo "ntpq command not found, please install the ntp package."
    exit 1
fi

# Initialize variables
all_zero=true
ntp_problem=false

# Get the output of ntpq -p
ntpq_output=$(ntpq -p)

# Parse the output
while read -r line; do
    # Skip the header
    if [[ "$line" == *"="* ]]; then
        continue
    fi

    # Extract the reach and status columns
    reach=$(echo "$line" | awk '{print $7}')
    status=$(echo "$line" | awk '{print $8}')

    #echo "reach=$reach"

    if [ "$reach" -ne 0 ]; then
        all_zero=false
    fi
    if [[ "$status" == *"INIT"* || "$status" == *"XFAC"* ]]; then
        ntp_problem=true
    fi
done <<< "$(echo "$ntpq_output" | awk 'NR>2')"

# Determine if the NTP service is healthy
if $all_zero; then
    echo "NTP service is not healthy: all servers have a reach value of 0."
elif $ntp_problem; then
    echo "NTP service is not healthy: detected INIT or XFAC status."
else
    echo "NTP service is healthy."
    exit 0
fi

# Check routing table
route_status=$(route -n | grep '169.254.0.0.*eth0')

if [ -z "$route_status" ]; then
    echo "Route 169.254.0.0 to eth0 not found. restart eth0 to resolve..."
    ifdown eth0 && ifup eth0
fi

# enhance ntp.conf
has_chdfs_route=$(cat ${route_conf_file} | grep "169\.254\..*eth1")
is_ntp_eth0=$(cat ${ntp_conf_file} | grep "interface listen eth0")
is_ntp_eth1=$(cat ${ntp_conf_file} | grep "interface listen eth1")
if [[ -n "${has_chdfs_route}" && -n "${is_ntp_eth0}" ]]; then
  cp -f "${ntp_conf_file}" "${ntp_conf_file}.${back_suffix}"
  sed -i "s/interface listen eth0/interface listen eth1/" "${ntp_conf_file}"
  echo "Updated interface listen eth0 to eth1 in ${ntp_conf_file}"
fi

if [[ -z "${has_chdfs_route}" && -n "${is_ntp_eth1}" ]]; then
  cp -f "${ntp_conf_file}" "${ntp_conf_file}.${back_suffix}"
  sed -i "s/interface listen eth1/interface listen eth0/" "${ntp_conf_file}"
  echo "Updated interface listen eth1 to eth0 in ${ntp_conf_file}"
fi

restart_ntp_service

