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

#include "resource_group.h"
#include "pipeline/pipeline_task.h"

namespace doris {
namespace resourcegroup {

pipeline::PipelineTask* ResourceGroupEntry::take() {
//    std::unique_lock<std::mutex> lock(_work_size_mutex);
    if (_queue.empty()) {
        return nullptr;
    }
    auto task = _queue.front();
    _queue.pop();
    return task;
}

void ResourceGroupEntry::incr_runtime_ns(int64_t runtime_ns)  {
    auto v_time = runtime_ns / _rs->cpu_share();
    _vruntime_ns += v_time;
    LOG(INFO) << cpu_share() << " inc " << v_time << " total: " << _vruntime_ns;
}

void ResourceGroupEntry::push_back(pipeline::PipelineTask* task) {
//    std::unique_lock<std::mutex> lock(_work_size_mutex);
    _queue.emplace(task);
}

int ResourceGroupEntry::cpu_share() const {
    return _rs->cpu_share();
}

ResourceGroup::ResourceGroup(uint64_t id, std::string name, int cpu_share)
        : _id(id), _name(name), _cpu_share(cpu_share), _task_entry(this) {}

} // namespace resourcegroup
} // namespace doris