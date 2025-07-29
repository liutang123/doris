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

#include "setats_reader.h"

#include <vector>

#include "common/status.h"
#include "util/deletion_vector.h"

namespace doris::vectorized {
SetatsReader::SetatsReader(std::unique_ptr<GenericReader> file_format_reader,
                           RuntimeProfile* profile, RuntimeState* state)
        : TableFormatReader(std::move(file_format_reader)),
          _profile(profile),
          _state(state) {
    static const char* Setats_profile = "SetatsProfile";
    ADD_TIMER(_profile, Setats_profile);
    _setats_profile.num_delete_rows =
            ADD_CHILD_COUNTER(_profile, "NumDeleteRows", TUnit::UNIT, Setats_profile);
}

Status SetatsReader::init_row_filters(const TFileRangeDesc& range, io::IOContext* io_ctx) {
    const auto& table_desc = range.table_format_params.setats_params;
    if (!table_desc.__isset.deletion_vector) {
        return Status::OK();
    }

    // set push down agg type to NONE because we can not do count push down opt
    // if there are delete files.
    _file_format_reader->set_push_down_agg_type(TPushAggOp::NONE);

    const auto& deletion_vector = table_desc.deletion_vector;
    uint32_t magic_number;
    std::memcpy(reinterpret_cast<char*>(&magic_number), deletion_vector.data(), 4);
    // change byte order to big endian
    std::reverse(reinterpret_cast<char*>(&magic_number),
                 reinterpret_cast<char*>(&magic_number) + 4);
    if (magic_number != 1991021888) {
        return Status::RuntimeError(
                "Setats's DeletionVector deserialize error: invalid magic number {}", magic_number);
    }

    roaring::Roaring roaring_bitmap;
    try {
        roaring_bitmap = roaring::Roaring::readSafe(deletion_vector.data() + 4, deletion_vector.size() - 4);
    } catch (std::runtime_error) {
        return Status::RuntimeError(
                "Setats's DeletionVector deserialize error: failed to deserialize roaring bitmap");
    }

    if (!roaring_bitmap.isEmpty()) {
        roaring_bitmap.iterate(
                [](uint32_t value, void* param) {
                    ((std::vector<int64_t> *)param)->push_back(value);
                    return true;
                }, &_delete_rows);
        COUNTER_UPDATE(_setats_profile.num_delete_rows, _delete_rows.size());
        set_delete_rows();

    }
    return Status::OK();
}
} // namespace doris::vectorized
