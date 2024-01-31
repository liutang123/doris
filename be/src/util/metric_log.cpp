// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/metric_log.h"

#include <cctype>
#include <chrono>
#include <sstream>
#include "spdlog/spdlog.h"
#include "spdlog/sinks/rotating_file_sink.h"

#include "common/config.h"
#include "common/logging.h"
#include "util/network_util.h"

namespace doris {

Status init_metric_log() {
    LOG(INFO) << "initializing metric logging...";
    std::string hostname;
    RETURN_IF_ERROR(get_hostname(&hostname));

    spdlog::rotating_logger_mt("metric_logger",
                               config::metric_log_dir + "/metric.log",
                               config::spdlog_file_size,
                               config::spdlog_max_files);
    spdlog::rotating_logger_mt("loads_logger",
                               config::loads_log_dir + "/loads.log",
                               config::spdlog_file_size,
                               config::spdlog_max_files);
    spdlog::rotating_logger_mt("profile_logger",
                               config::profile_log_dir + "/profile.log",
                               config::spdlog_file_size,
                               config::spdlog_max_files);
    std::shared_ptr<spdlog::logger> new_profile_logger = spdlog::rotating_logger_mt("new_profile_logger",
                               config::new_profile_log_dir + "/profile.log",
                               config::spdlog_file_size,
                               config::spdlog_max_files);

    // XMD log format: https://km.sankuai.com/page/28116726
    // yyyy-MM-dd HH::mm::ss.SSS host appkey [level] thread logger_name #XMDT#{k1=v1 k2=v2 }#XMDT#
    std::stringstream tag_pattern;
    tag_pattern << "%Y-%m-%d %H:%M:%S.%e " << hostname
            << " null [INFO] %t %n #XMDT#{domain=" << config::mt_domain
            << "%v}#XMDT#"; /// WARN: input one space at the beginning of a log
    spdlog::set_pattern(tag_pattern.str());
    spdlog::flush_every(std::chrono::seconds(3));

    // yyyy-MM-dd HH::mm::ss.SSS host appkey [level] thread logger_name #XMDJ#{"k1": "v1"}#XMDJ#
    std::stringstream json_pattern;
    json_pattern << "%Y-%m-%d %H:%M:%S.%e " << hostname
                 << " null [INFO] %t %n #XMDJ#%v#XMDJ#";
    new_profile_logger->set_pattern(json_pattern.str());
    return Status::OK();
}

void emit_metric_log(const MetricLog& log, int64_t emit_threshold) {
    if (log.value <= emit_threshold) {
        return;
    }
    std::stringstream ss;
    ss << " metric=" << log.metric
       << " value=" << log.value
       << " db_name=" << log.db_name
       << " query_type=" << log.query_type
       << " table_name=" << log.table_name
       << " query_id=" << log.query_id
       << " fragment_id=" << log.fragment_id
       << " thread=" << log.thread
       << " tablet=" << log.tablet;
    spdlog::get("metric_logger")->info(ss.str());
}

void emit_loads_log(const LoadsLog& load) {
    std::stringstream ss;
    ss << " db=" << load.db
       << " table=" << load.table
       << " label=" << load.label
       << " status=" << load.status
       << " cluster=" << (load.cluster.empty() ? "default_cluster" : load.cluster)
       << " user=" << load.user
       << " user_ip=" << load.user_ip
       << " receive_bytes=" << load.receive_bytes
       << " number_loaded_rows=" << load.number_loaded_rows
       << " number_filtered_rows=" << load.number_filtered_rows
       << " load_cost_ms=" << load.load_cost_ms
       << " load_type=" << load.load_type;
    spdlog::get("loads_logger")->info(ss.str());
}

void put_format_k(std::stringstream& buf, const std::string& str) {
#define INIT        0x01u
#define OTHER_CHAR  0x02u
#define UPPER_CHAR  0x04u
    uint32_t state = INIT;
    for (auto c : str) {
        if (islower(c)) {
            if (state & OTHER_CHAR) {
                buf.put('_');
            }
            buf.put(c);
            state = 0;
        } else if (isupper(c)) {
            if (!(state & (INIT | UPPER_CHAR))) {
                buf.put('_');
            }
            buf.put(tolower(c));
            state = UPPER_CHAR;
        } else if (isdigit(c)) {
            if (state & (INIT | OTHER_CHAR)) {
                buf.put('_');
            }
            buf.put(c);
            state = 0;
        } else {
            state = (state & INIT) | OTHER_CHAR;
        }
    }
}

void put_format_v(std::stringstream& buf, const std::string& str) {
    for (auto c : str) {
        if (c == '=' || c == '\n' || c == '\t' || c == '\r') {
            buf.put(' ');
        } else {
            buf.put(c);
        }
    }
}

XMDLog& XMDLog::tag(const std::string& key, const std::string& value) {
    _buf.put(' ');
    _buf << key;
    _buf.put('=');
    _buf << value;
    return *this;
}

XMDLog& XMDLog::tag_format_k(const std::string& key, const std::string& value) {
    _buf.put(' ');
    put_format_k(_buf, key);
    _buf.put('=');
    _buf << value;
    return *this;
}

XMDLog& XMDLog::tag_format_v(const std::string& key, const std::string& value) {
    _buf.put(' ');
    _buf << key;
    _buf.put('=');
    put_format_v(_buf, value);
    return *this;
}

XMDLog& XMDLog::tag_format_kv(const std::string& key, const std::string& value) {
    _buf.put(' ');
    put_format_k(_buf, key);
    _buf.put('=');
    put_format_v(_buf, value);
    return *this;
}

XMDLog& XMDLog::json(const std::string& json_str) {
    _buf << json_str;
    return *this;
}

void XMDLog::log() {
    spdlog::get(_logger)->info(_buf.str());
}
} // namespace doris