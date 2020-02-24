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

#include <memory> // for unique_ptr

#include "common/status.h" // for Status
#include "gen_cpp/segment_v2.pb.h" // for EncodingTypePB
#include "olap/rowset/segment_v2/column_zone_map.h" // for ColumnZoneMapBuilder
#include "olap/collection.h"
#include "olap/rowset/segment_v2/common.h" // for rowid_t
#include "olap/rowset/segment_v2/page_pointer.h" // for PagePointer
#include "util/bitmap.h" // for BitmapChange
#include "util/slice.h" // for OwnedSlice

namespace doris {

class TypeInfo;
class WritableFile;
class BlockCompressionCodec;

namespace segment_v2 {

struct ColumnWriterOptions {
    EncodingTypePB encoding_type = DEFAULT_ENCODING;
    CompressionTypePB compression_type = segment_v2::CompressionTypePB::LZ4F;
    size_t data_page_size = 64 * 1024;
    // store compressed page only when space saving is above the threshold.
    // space saving = 1 - compressed_size / uncompressed_size
    double compression_min_space_saving = 0.1;
    bool need_zone_map = false;
    bool need_bitmap_index = false;
    bool need_bloom_filter = false;
};

class BitmapIndexWriter;
class EncodingInfo;
class NullBitmapBuilder;
class OrdinalPageIndexBuilder;
class PageBuilder;
class BloomFilterIndexWriter;

// Encode one column's data into some memory slice.
// Because some columns would be stored in a file, we should wait
// until all columns has been finished, and then data can be written
// to file
class ColumnWriter {
public:
    static Status create(const ColumnWriterOptions& opts,
                         std::unique_ptr<Field> field,
                         bool is_nullable,
                         WritableFile* output_file,
                         std::unique_ptr<ColumnWriter>* writer);

    static Status create(const ColumnWriterOptions& opts,
                         const TabletColumn* column,
                         WritableFile* output_file,
                         std::unique_ptr<ColumnWriter>* writer);

    ~ColumnWriter();

    virtual Status init();

    template<typename CellType>
    Status append(const CellType& cell) {
        if (_is_nullable) {
            uint8_t nullmap = 0;
            BitmapChange(&nullmap, 0, cell.is_null());
            return append_nullable(&nullmap, cell.cell_ptr(), 1);
        } else {
            return append(cell.cell_ptr(), 1);
        }
    }

    // Now we only support append one by one, we should support append
    // multi rows in one call
    Status append(bool is_null, void* data) {
        uint8_t nullmap = 0;
        BitmapChange(&nullmap, 0, is_null);
        return append_nullable(&nullmap, data, 1);
    }

    Status append_nulls(size_t num_rows);
    Status append(const void* data, size_t num_rows);
    Status append_nullable(const uint8_t* nullmap, const void* data, size_t num_rows);
    Status append_nullable_by_null_signs(const bool* null_signs, const void* data, size_t num_rows) {
        const auto* ptr = (const uint8_t*)data;
        for (size_t i = 0; i < num_rows; ++i) {
            if (null_signs[i]) {
                RETURN_IF_ERROR(append_nulls(1));
            } else {
                RETURN_IF_ERROR(append(ptr, 1));
            }
            ptr += _field->size();
        }
    }

    virtual uint64_t estimate_buffer_size();

    // finish append data
    virtual Status finish();

    // write all data into file
    virtual Status write_data();
    virtual Status write_ordinal_index();
    Status write_zone_map();
    Status write_bitmap_index();
    Status write_bloom_filter_index();
    virtual void write_meta(ColumnMetaPB* meta);

protected:
    ColumnWriter(const ColumnWriterOptions& opts,
                 std::unique_ptr<Field> field,
                 bool is_nullable,
                 WritableFile* output_file);
    // used in init() for create page builder.
    virtual Status create_page_builder(const EncodingInfo* encoding_info, PageBuilder** page_builder);

    // used for append not null data.
    virtual Status _append_data(const uint8_t** ptr, size_t num_rows);

    Status _finish_current_page();

    virtual Status put_page_footer_info(faststring* header) {};

    std::unique_ptr<PageBuilder> _page_builder;

    std::unique_ptr<NullBitmapBuilder> _null_bitmap_builder;

    ColumnWriterOptions _opts;

    bool _is_nullable;

    rowid_t _next_rowid = 0;

private:

    // All Pages will be organized into a linked list
    struct Page {
        int32_t first_rowid;
        int32_t num_rows;
        // the data vector may contain:
        //     1. one OwnedSlice if the data is compressed
        //     2. one OwnedSlice if the data is not compressed and is not nullable
        //     3. two OwnedSlice if the data is not compressed and is nullable
        // use vector for easier management for lifetime of OwnedSlice
        std::vector<OwnedSlice> data;
        Page* next = nullptr;
    };

    struct PageHead {
        Page* head = nullptr;
        Page* tail = nullptr;
    };

    void _push_back_page(Page* page) {
        // add page to pages' tail
        if (_pages.tail != nullptr) {
            _pages.tail->next = page;
        }
        _pages.tail = page;
        if (_pages.head == nullptr) {
            _pages.head = page;
        }
        for (auto& data_slice : page->data) {
            _data_size += data_slice.slice().size;
        }
    }

    Status _write_raw_data(const std::vector<Slice>& data, size_t* bytes_written);

    Status _write_data_page(Page* page);
    Status _compress_and_write_page(std::vector<Slice>* origin_data, PagePointer* pp);
    Status _write_physical_page(std::vector<Slice>* origin_data, PagePointer* pp);

private:
    WritableFile* _output_file = nullptr;

    // cached generated pages,
    PageHead _pages;
    rowid_t _last_first_rowid = 0;

    const EncodingInfo* _encoding_info = nullptr;
    const BlockCompressionCodec* _compress_codec = nullptr;

    std::unique_ptr<OrdinalPageIndexBuilder> _ordinal_index_builder;
    std::unique_ptr<ColumnZoneMapBuilder> _column_zone_map_builder;
    std::unique_ptr<Field> _field;
    std::unique_ptr<BitmapIndexWriter> _bitmap_index_builder;
    std::unique_ptr<BloomFilterIndexWriter> _bloom_filter_index_builder;
    BitmapIndexColumnPB _bitmap_index_meta;
    BloomFilterIndexPB _bloom_filter_index_meta;

    PagePointer _ordinal_index_pp;
    PagePointer _zone_map_pp;
    PagePointer _dict_page_pp;
    // the total data size of page list
    uint64_t _data_size;
    uint64_t _written_size = 0;
};

class ListColumnWriter : public ColumnWriter {
    ~ListColumnWriter();

    Status init() override ;

protected:
    // used in init() for create page builder.
    Status create_page_builder(const EncodingInfo* encoding_info, PageBuilder** page_builder) override;

    Status _append_data(const uint8_t** ptr, size_t num_rows) override;

    Status put_page_footer_info(faststring* header) override;

    uint64_t estimate_buffer_size() override ;
    Status finish() override ;
    Status write_data() override ;
    Status write_ordinal_index() override ;
    void write_meta(ColumnMetaPB* meta) override;

private:
    ListColumnWriter(const ColumnWriterOptions& opts,
                     std::unique_ptr<Field> field,
                     bool is_nullable,
                     WritableFile* output_file,
                     unique_ptr<ColumnWriter> _item_writer);

    unique_ptr<ColumnWriter> _item_writer;
    ordinal_t _next_item_ordinal = 0;

    friend class ColumnWriter;
};
}  // namespace segment_v2
}  // namespace doris
