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

#include "exchange_sink_buffer.h"

#include <brpc/controller.h>
#include <butil/errno.h>
#include <butil/iobuf_inl.h>
#include <fmt/format.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/callback.h>
#include <stddef.h>

#include <atomic>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <ostream>
#include <utility>

#include "common/status.h"
#include "pipeline/pipeline_fragment_context.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "util/proto_util.h"
#include "util/time.h"
#include "vec/sink/vdata_stream_sender.h"

namespace doris::pipeline {

ExchangeSinkBuffer::ExchangeSinkBuffer(PUniqueId query_id, PlanNodeId dest_node_id, int send_id,
                                       int be_number, PipelineFragmentContext* context)
        : _is_finishing(false),
          _query_id(query_id),
          _dest_node_id(dest_node_id),
          _sender_id(send_id),
          _be_number(be_number),
          _context(context) {}

ExchangeSinkBuffer::~ExchangeSinkBuffer() = default;

void ExchangeSinkBuffer::close() {
    _instance_to_broadcast_package_queue.clear();
    _instance_to_package_queue.clear();
    _instance_to_request.clear();
}

bool ExchangeSinkBuffer::can_write() const {
    size_t max_package_size = 64 * _instance_to_package_queue.size();
    size_t total_package_size = 0;
    for (auto& [_, q] : _instance_to_package_queue) {
        total_package_size += q.size();
    }
    return total_package_size <= max_package_size;
}

bool ExchangeSinkBuffer::is_pending_finish() {
    //note(wb) angly implementation here, because operator couples the scheduling logic
    // graceful implementation maybe as follows:
    // 1 make ExchangeSinkBuffer support try close which calls brpc::StartCancel
    // 2 make BlockScheduler calls tryclose when query is cancel
    bool need_cancel = _context->is_canceled();

    for (auto& pair : _instance_to_package_queue_mutex) {
        std::unique_lock lock(*(pair.second));
        auto& id = pair.first;
        if (!_instance_to_sending_by_pipeline.at(id)) {
            // when pending finish, we need check whether current query is cancelled
            if (need_cancel && _instance_to_rpc_ctx.find(id) != _instance_to_rpc_ctx.end()) {
                auto& rpc_ctx = _instance_to_rpc_ctx[id];
                if (!rpc_ctx.is_cancelled) {
                    brpc::StartCancel(rpc_ctx._closure->cntl.call_id());
                    rpc_ctx.is_cancelled = true;
                }
            }
            return true;
        }
    }
    return false;
}

void ExchangeSinkBuffer::register_sink(TUniqueId fragment_instance_id) {
    if (_is_finishing) {
        return;
    }
    auto low_id = fragment_instance_id.lo;
    if (_instance_to_package_queue_mutex.count(low_id)) {
        return;
    }
    _instance_to_package_queue_mutex[low_id] = std::make_unique<bthread::Mutex>();
    _instance_to_seq[low_id] = 0;
    _instance_to_package_queue[low_id] = std::queue<TransmitInfo, std::list<TransmitInfo>>();
    _instance_to_broadcast_package_queue[low_id] =
            std::queue<BroadcastTransmitInfo, std::list<BroadcastTransmitInfo>>();
    PUniqueId finst_id;
    finst_id.set_hi(fragment_instance_id.hi);
    finst_id.set_lo(fragment_instance_id.lo);
    _instance_to_sending_by_pipeline[low_id] = true;
    _instance_to_rpc_ctx[low_id] = {};
    _instance_watcher[low_id] = {};
    _instance_watcher[low_id].start();
    _instance_to_receiver_eof[low_id] = false;
    _instance_to_rpc_time[low_id] = 0;
    _instance_to_rpc_exec_delay_time[low_id] = 0;
    _instance_to_rpc_exec_time[low_id] = 0;
    _instance_to_rpc_callback_time[low_id] = 0;
    _instance_to_rpc_callback_exec_time[low_id] = 0;
    _construct_request(low_id, finst_id);
}

Status ExchangeSinkBuffer::add_block(TransmitInfo&& request) {
    if (_is_finishing) {
        return Status::OK();
    }
    TUniqueId ins_id = request.channel->_fragment_instance_id;
    bool send_now = false;
    {
        std::unique_lock lock(*_instance_to_package_queue_mutex[ins_id.lo]);
        // Do not have in process rpc, directly send
        if (_instance_to_sending_by_pipeline[ins_id.lo]) {
            send_now = true;
            _instance_to_sending_by_pipeline[ins_id.lo] = false;
        }
        _instance_to_package_queue[ins_id.lo].emplace(std::move(request));
    }
    if (send_now) {
        RETURN_IF_ERROR(_send_rpc(ins_id.lo));
    }

    return Status::OK();
}

Status ExchangeSinkBuffer::add_block(BroadcastTransmitInfo&& request) {
    if (_is_finishing) {
        return Status::OK();
    }
    TUniqueId ins_id = request.channel->_fragment_instance_id;
    if (_is_receiver_eof(ins_id.lo)) {
        return Status::EndOfFile("receiver eof");
    }
    bool send_now = false;
    request.block_holder->ref();
    {
        std::unique_lock lock(*_instance_to_package_queue_mutex[ins_id.lo]);
        // Do not have in process rpc, directly send
        if (_instance_to_sending_by_pipeline[ins_id.lo]) {
            send_now = true;
            _instance_to_sending_by_pipeline[ins_id.lo] = false;
        }
        _instance_to_broadcast_package_queue[ins_id.lo].emplace(request);
    }
    if (send_now) {
        RETURN_IF_ERROR(_send_rpc(ins_id.lo));
    }

    return Status::OK();
}

Status ExchangeSinkBuffer::_send_rpc(InstanceLoId id) {
    std::unique_lock lock(*_instance_to_package_queue_mutex[id]);

    DCHECK(_instance_to_sending_by_pipeline[id] == false);

    std::queue<TransmitInfo, std::list<TransmitInfo>>& q = _instance_to_package_queue[id];
    std::queue<BroadcastTransmitInfo, std::list<BroadcastTransmitInfo>>& broadcast_q =
            _instance_to_broadcast_package_queue[id];

    if (_is_finishing) {
        _instance_to_sending_by_pipeline[id] = true;
        return Status::OK();
    }

    if (!q.empty()) {
        // If we have data to shuffle which is not broadcasted
        auto& request = q.front();
        auto& brpc_request = _instance_to_request[id];
        brpc_request->set_eos(request.eos);
        brpc_request->set_packet_seq(_instance_to_seq[id]++);
        if (request.block) {
            brpc_request->set_allocated_block(request.block.get());
        }
        auto* closure = request.channel->get_closure(id, request.eos, nullptr);

        _instance_to_rpc_ctx[id]._closure = closure;
        _instance_to_rpc_ctx[id].is_cancelled = false;

        closure->cntl.set_timeout_ms(request.channel->_brpc_timeout_ms);
        if (config::exchange_sink_ignore_eovercrowded) {
            closure->cntl.ignore_eovercrowded();
        }
        closure->addFailedHandler(
                [&](const InstanceLoId& id, const std::string& err) { _failed(id, err); });
        closure->start_rpc_time = GetCurrentTimeNanos();
        closure->addSuccessHandler([&](const InstanceLoId& id, const bool& eos,
                                       const PTransmitDataResult& result,
                                       const int64_t& start_rpc_time) {
            auto callback_start_time = GetCurrentTimeNanos();
            Status s(Status::create(result.status()));
            if (s.is<ErrorCode::END_OF_FILE>()) {
                _set_receiver_eof(id);
            } else if (!s.ok()) {
                _failed(id,
                        fmt::format("exchange req success but status isn't ok: {}", s.to_string()));
            } else if (eos) {
                _ended(id);
            } else {
                _send_rpc(id);
            }
            auto callback_end_time = GetCurrentTimeNanos();
            set_rpc_time(id, start_rpc_time, result.receive_time(), result.exec_start_time(),
                         result.exec_end_time(), callback_start_time, callback_end_time);
        });
        {
            SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->orphan_mem_tracker());
            if (enable_http_send_block(*brpc_request)) {
                RETURN_IF_ERROR(transmit_block_http(_context->get_runtime_state(), closure,
                                                    *brpc_request,
                                                    request.channel->_brpc_dest_addr));
            } else {
                transmit_block(*request.channel->_brpc_stub, closure, *brpc_request);
            }
        }
        if (request.block) {
            brpc_request->release_block();
        }
        q.pop();
    } else if (!broadcast_q.empty()) {
        // If we have data to shuffle which is broadcasted
        auto& request = broadcast_q.front();
        auto& brpc_request = _instance_to_request[id];
        brpc_request->set_eos(request.eos);
        brpc_request->set_packet_seq(_instance_to_seq[id]++);
        if (request.block_holder->get_block()) {
            brpc_request->set_allocated_block(request.block_holder->get_block());
        }
        auto* closure = request.channel->get_closure(id, request.eos, request.block_holder);

        ExchangeRpcContext rpc_ctx;
        rpc_ctx._closure = closure;
        rpc_ctx.is_cancelled = false;
        _instance_to_rpc_ctx[id] = rpc_ctx;

        closure->cntl.set_timeout_ms(request.channel->_brpc_timeout_ms);
        if (config::exchange_sink_ignore_eovercrowded) {
            closure->cntl.ignore_eovercrowded();
        }
        closure->addFailedHandler(
                [&](const InstanceLoId& id, const std::string& err) { _failed(id, err); });
        closure->start_rpc_time = GetCurrentTimeNanos();
        closure->addSuccessHandler([&](const InstanceLoId& id, const bool& eos,
                                       const PTransmitDataResult& result,
                                       const int64_t& start_rpc_time) {
            auto callback_start_time = GetCurrentTimeNanos();
            Status s(Status::create(result.status()));
            if (s.is<ErrorCode::END_OF_FILE>()) {
                _set_receiver_eof(id);
            } else if (!s.ok()) {
                _failed(id,
                        fmt::format("exchange req success but status isn't ok: {}", s.to_string()));
            } else if (eos) {
                _ended(id);
            } else {
                _send_rpc(id);
            }
            auto callback_end_time = GetCurrentTimeNanos();
            set_rpc_time(id, start_rpc_time, result.receive_time(), result.exec_start_time(),
                         result.exec_end_time(), callback_start_time, callback_end_time);
        });
        {
            SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->orphan_mem_tracker());
            if (enable_http_send_block(*brpc_request)) {
                RETURN_IF_ERROR(transmit_block_http(_context->get_runtime_state(), closure,
                                                    *brpc_request,
                                                    request.channel->_brpc_dest_addr));
            } else {
                transmit_block(*request.channel->_brpc_stub, closure, *brpc_request);
            }
        }
        if (request.block_holder->get_block()) {
            brpc_request->release_block();
        }
        broadcast_q.pop();
    } else {
        _instance_to_sending_by_pipeline[id] = true;
    }

    return Status::OK();
}

void ExchangeSinkBuffer::_construct_request(InstanceLoId id, PUniqueId finst_id) {
    _instance_to_request[id] = std::make_unique<PTransmitDataParams>();
    _instance_to_request[id]->mutable_finst_id()->CopyFrom(finst_id);
    _instance_to_request[id]->mutable_query_id()->CopyFrom(_query_id);

    _instance_to_request[id]->set_node_id(_dest_node_id);
    _instance_to_request[id]->set_sender_id(_sender_id);
    _instance_to_request[id]->set_be_number(_be_number);
}

void ExchangeSinkBuffer::_ended(InstanceLoId id) {
    std::unique_lock lock(*_instance_to_package_queue_mutex[id]);
    _instance_to_sending_by_pipeline[id] = true;
    _instance_watcher[id].stop();
}

void ExchangeSinkBuffer::_failed(InstanceLoId id, const std::string& err) {
    _is_finishing = true;
    _context->cancel(PPlanFragmentCancelReason::INTERNAL_ERROR, err);
    _ended(id);
}

void ExchangeSinkBuffer::_set_receiver_eof(InstanceLoId id) {
    std::unique_lock lock(*_instance_to_package_queue_mutex[id]);
    _instance_to_receiver_eof[id] = true;
    _instance_to_sending_by_pipeline[id] = true;
}

bool ExchangeSinkBuffer::_is_receiver_eof(InstanceLoId id) {
    std::unique_lock lock(*_instance_to_package_queue_mutex[id]);
    return _instance_to_receiver_eof[id];
}

void ExchangeSinkBuffer::get_max_min_rpc_time(int64_t* max_time, int64_t* min_time,
                                              int64_t* max_exec_delay_time,
                                              int64_t* min_exec_delay_time, int64_t* max_exec_time,
                                              int64_t* min_exec_time, int64_t* max_callback_time,
                                              int64_t* min_callback_time,
                                              int64_t* max_callback_exec_time,
                                              int64_t* min_callback_exec_time) {
    int64_t local_max_time = 0;
    int64_t local_max_exec_delay_time = 0;
    int64_t local_max_exec_time = 0;
    int64_t local_max_callback_time = 0;
    int64_t local_max_callback_exec_time = 0;
    int64_t local_min_time = INT64_MAX;
    int64_t local_min_exec_delay_time = INT64_MAX;
    int64_t local_min_exec_time = INT64_MAX;
    int64_t local_min_callback_time = INT64_MAX;
    int64_t local_min_callback_exec_time = INT64_MAX;
    for (auto& [id, time] : _instance_to_rpc_time) {
        if (time != 0) {
            local_max_time = std::max(local_max_time, time);
            local_min_time = std::min(local_min_time, time);
        }
        auto& exec_delay_time = _instance_to_rpc_exec_delay_time[id];
        if (exec_delay_time != 0) {
            local_max_exec_delay_time = std::max(local_max_exec_delay_time, exec_delay_time);
            local_min_exec_delay_time = std::min(local_min_exec_delay_time, exec_delay_time);
        }
        auto& exec_time = _instance_to_rpc_exec_time[id];
        if (exec_time != 0) {
            local_max_exec_time = std::max(local_max_exec_time, exec_time);
            local_min_exec_time = std::min(local_min_exec_time, exec_time);
        }
        auto& callback_time = _instance_to_rpc_callback_time[id];
        if (callback_time != 0) {
            local_max_callback_time = std::max(local_max_callback_time, callback_time);
            local_min_callback_time = std::min(local_min_callback_time, callback_time);
        }
        auto& callback_exec_time = _instance_to_rpc_callback_exec_time[id];
        if (callback_exec_time != 0) {
            local_max_callback_exec_time =
                    std::max(local_max_callback_exec_time, callback_exec_time);
            local_min_callback_exec_time =
                    std::min(local_min_callback_exec_time, callback_exec_time);
        }
    }
    *max_time = local_max_time;
    *max_exec_delay_time = local_max_exec_delay_time;
    *max_exec_time = local_max_exec_time;
    *max_callback_time = local_max_callback_time;
    *max_callback_exec_time = local_max_callback_exec_time;
    *min_time = local_min_time;
    *min_exec_delay_time = local_min_exec_delay_time;
    *min_exec_time = local_min_exec_time;
    *min_callback_time = local_min_callback_time;
    *min_callback_exec_time = local_min_callback_exec_time;
}

int64_t ExchangeSinkBuffer::get_sum_rpc_time() {
    int64_t sum_time = 0;
    for (auto& [id, time] : _instance_to_rpc_time) {
        sum_time += time;
    }
    return sum_time;
}

void ExchangeSinkBuffer::set_rpc_time(InstanceLoId id, int64_t start_rpc_time, int64_t receive_time,
                                      int64_t exec_start, int64_t exec_end,
                                      int64_t callback_start_time, int64_t callback_end_time) {
    _rpc_count++;
    DCHECK(_instance_to_rpc_time.find(id) != _instance_to_rpc_time.end());
    int64_t rpc_forward_time = receive_time - start_rpc_time;
    int64_t rpc_exec_delay_time = exec_start - receive_time;
    int64_t rpc_exec_time = exec_end - exec_start;
    int64_t rpc_callback_time = callback_start_time - exec_end;
    int64_t callback_exec_time = callback_end_time - callback_start_time;
    if (rpc_forward_time > 0) {
        _instance_to_rpc_time[id] += rpc_forward_time;
    }
    if (rpc_exec_delay_time > 0) {
        _instance_to_rpc_exec_delay_time[id] += rpc_exec_delay_time;
    }
    if (rpc_exec_time > 0) {
        _instance_to_rpc_exec_time[id] += rpc_exec_time;
    }
    if (rpc_callback_time > 0) {
        _instance_to_rpc_callback_time[id] += rpc_callback_time;
    }
    if (callback_exec_time > 0) {
        _instance_to_rpc_callback_exec_time[id] += callback_exec_time;
    }
}

void ExchangeSinkBuffer::update_profile(RuntimeProfile* profile) {
    auto* _count_rpc = ADD_COUNTER(profile, "Rpc0Count", TUnit::UNIT);
    auto* _max_rpc_timer = ADD_TIMER(profile, "Rpc1MaxTime");
    auto* _min_rpc_timer = ADD_TIMER(profile, "Rpc1MinTime");
    auto* _sum_rpc_timer = ADD_TIMER(profile, "Rpc1SumTime");
    auto* _avg_rpc_timer = ADD_TIMER(profile, "Rpc1AvgTime");

    auto* _max_rpc_exec_delay_timer = ADD_TIMER(profile, "Rpc2MaxExecDelayTime");
    auto* _min_rpc_exec_delay_timer = ADD_TIMER(profile, "Rpc2MinExecDelayTime");

    auto* _max_rpc_exec_timer = ADD_TIMER(profile, "Rpc3MaxExecTime");
    auto* _min_rpc_exec_timer = ADD_TIMER(profile, "Rpc3MinExecTime");

    auto* _max_rpc_callback_timer = ADD_TIMER(profile, "Rpc4MaxCallbackTime");
    auto* _min_rpc_callback_timer = ADD_TIMER(profile, "Rpc4MinCallbackTime");

    auto* _max_rpc_callback_exec_timer = ADD_TIMER(profile, "Rpc5MaxCallbackExecTime");
    auto* _min_rpc_callback_exec_timer = ADD_TIMER(profile, "Rpc5MinCallbackExecTime");

    int64_t max_rpc_time = 0, min_rpc_time = 0, max_exec_delay_t = 0, min_exec_delay_t = 0,
            max_exec_t, min_exec_t = 0, max_callback_t = 0, min_callback_t = 0,
            max_callback_exec_t = 0, min_callback_exec_t = 0;
    get_max_min_rpc_time(&max_rpc_time, &min_rpc_time, &max_exec_delay_t, &min_exec_delay_t,
                         &max_exec_t, &min_exec_t, &max_callback_t, &min_callback_t,
                         &max_callback_exec_t, &min_callback_exec_t);
    _max_rpc_timer->set(max_rpc_time);
    _min_rpc_timer->set(min_rpc_time);
    _max_rpc_exec_delay_timer->set(max_exec_delay_t);
    _min_rpc_exec_delay_timer->set(min_exec_delay_t);
    _max_rpc_exec_timer->set(max_exec_t);
    _min_rpc_exec_timer->set(min_exec_t);

    _max_rpc_callback_timer->set(max_callback_t);
    _min_rpc_callback_timer->set(min_callback_t);
    _max_rpc_callback_exec_timer->set(max_callback_exec_t);
    _min_rpc_callback_exec_timer->set(min_callback_exec_t);

    _count_rpc->set(_rpc_count);
    int64_t sum_time = get_sum_rpc_time();
    _sum_rpc_timer->set(sum_time);
    _avg_rpc_timer->set(sum_time / std::max(static_cast<int64_t>(1), _rpc_count.load()));

    uint64_t max_end_time = 0;
    for (auto& [id, timer] : _instance_watcher) {
        max_end_time = std::max(timer.elapsed_time(), max_end_time);
    }
    auto* _max_end_timer = ADD_TIMER(profile, "MaxRpcEndTime");
    _max_end_timer->set(static_cast<int64_t>(max_end_time));
}
} // namespace doris::pipeline
