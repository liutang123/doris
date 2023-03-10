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
#pragma once

#include <shared_mutex>
#include <unordered_map>

#include "task_group.h"

namespace doris::taskgroup {

class TaskGroupManager {
    DECLARE_SINGLETON(TaskGroupManager)
public:
    // TODO pipeline task group
    TaskGroupPtr get_or_create_task_group(uint64_t id);

    static constexpr uint64_t DEFAULT_RG_ID = 0;
    static constexpr int DEFAULT_CPU_SHARE = 64;

    static constexpr uint64_t POC_RG_ID = 1;
    static constexpr int POC_RG_CPU_SHARE = 128;

private:
    void _create_default_task_group();
    // TODO pipeline task group remote this after POC
    void _create_poc_task_group();

    std::shared_mutex _group_mutex;
    std::unordered_map<uint64_t, TaskGroupPtr> _task_groups;
};

}

