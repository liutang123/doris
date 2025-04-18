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

#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <gflags/gflags.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <sstream>
#include <string>

#include "common/status.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "io/fs/file_reader.h"
#include "io/fs/local_file_system.h"
#include "json2pb/pb_to_json.h"
#include "olap/data_dir.h"
#include "olap/olap_define.h"
#include "olap/options.h"
#include "olap/rowset/segment_v2/binary_plain_page.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/indexed_column_reader.h"
#include "olap/rowset/segment_v2/page_io.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/utils.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/jsonb_writer.h"

using doris::JsonbWriter;
using std::filesystem::path;
using doris::DataDir;
using doris::OlapMeta;
using doris::OlapReaderStatistics;
using doris::Status;
using doris::TabletMeta;
using doris::TabletMetaManager;
using doris::Slice;
using strings::Substitute;
using doris::segment_v2::SegmentFooterPB;
using doris::segment_v2::ColumnReader;
using doris::segment_v2::PageHandle;
using doris::segment_v2::PagePointer;
using doris::segment_v2::ColumnMetaPB;
using doris::segment_v2::ColumnReaderOptions;
using doris::segment_v2::ColumnIteratorOptions;
using doris::segment_v2::PageFooterPB;
using doris::segment_v2::PageReadOptions;
using doris::segment_v2::PageTypePB;
using doris::io::FileReaderSPtr;

const std::string HEADER_PREFIX = "tabletmeta_";

DEFINE_string(root_path, "", "storage root path");
DEFINE_string(operation, "get_meta",
              "valid operation: get_meta, flag, load_meta, delete_meta, show_meta");
DEFINE_int64(tablet_id, 0, "tablet_id for tablet meta");
DEFINE_int32(schema_hash, 0, "schema_hash for tablet meta");
DEFINE_string(json_meta_path, "", "absolute json meta file path");
DEFINE_string(pb_meta_path, "", "pb meta file path");
DEFINE_string(tablet_file, "", "file to save a set of tablets");
DEFINE_string(file, "", "segment file path");
DEFINE_bool(show_column, false, "show column meta if config to true");

std::string get_usage(const std::string& progname) {
    std::stringstream ss;
    ss << progname << " is the Doris BE Meta tool.\n";
    ss << "Stop BE first before use this tool.\n";
    ss << "Usage:\n";
    ss << "./meta_tool --operation=get_meta --root_path=/path/to/storage/path "
          "--tablet_id=tabletid --schema_hash=schemahash\n";
    ss << "./meta_tool --operation=load_meta --root_path=/path/to/storage/path "
          "--json_meta_path=path\n";
    ss << "./meta_tool --operation=delete_meta "
          "--root_path=/path/to/storage/path --tablet_id=tabletid "
          "--schema_hash=schemahash\n";
    ss << "./meta_tool --operation=delete_meta --tablet_file=file_path\n";
    ss << "./meta_tool --operation=show_meta --pb_meta_path=path\n";
    ss << "./meta_tool --operation=show_segment_footer --file=/path/to/segment/file\n";
    return ss.str();
}

void show_meta() {
    TabletMeta tablet_meta;
    Status s = tablet_meta.create_from_file(FLAGS_pb_meta_path);
    if (!s.ok()) {
        std::cout << "load pb meta file:" << FLAGS_pb_meta_path << " failed"
                  << ", status:" << s << std::endl;
        return;
    }
    std::string json_meta;
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    doris::TabletMetaPB tablet_meta_pb;
    tablet_meta.to_meta_pb(&tablet_meta_pb);
    json2pb::ProtoMessageToJson(tablet_meta_pb, &json_meta, json_options);
    std::cout << json_meta << std::endl;
}

void get_meta(DataDir* data_dir) {
    std::string value;
    Status s =
            TabletMetaManager::get_json_meta(data_dir, FLAGS_tablet_id, FLAGS_schema_hash, &value);
    if (s.is<doris::ErrorCode::META_KEY_NOT_FOUND>()) {
        std::cout << "no tablet meta for tablet_id:" << FLAGS_tablet_id
                  << ", schema_hash:" << FLAGS_schema_hash << std::endl;
        return;
    }
    std::cout << value << std::endl;
}

void load_meta(DataDir* data_dir) {
    // load json tablet meta into meta
    Status s = TabletMetaManager::load_json_meta(data_dir, FLAGS_json_meta_path);
    if (!s.ok()) {
        std::cout << "load meta failed, status:" << s << std::endl;
        return;
    }
    std::cout << "load meta successfully" << std::endl;
}

void delete_meta(DataDir* data_dir) {
    Status s = TabletMetaManager::remove(data_dir, FLAGS_tablet_id, FLAGS_schema_hash);
    if (!s.ok()) {
        std::cout << "delete tablet meta failed for tablet_id:" << FLAGS_tablet_id
                  << ", schema_hash:" << FLAGS_schema_hash << ", status:" << s << std::endl;
        return;
    }
    std::cout << "delete meta successfully" << std::endl;
}

Status init_data_dir(const std::string& dir, std::unique_ptr<DataDir>* ret) {
    std::string root_path;
    RETURN_IF_ERROR(doris::io::global_local_filesystem()->canonicalize(dir, &root_path));
    doris::StorePath path;
    auto res = parse_root_path(root_path, &path);
    if (!res.ok()) {
        std::cout << "parse root path failed:" << root_path << std::endl;
        return Status::InternalError("parse root path failed");
    }

    std::unique_ptr<DataDir> p(
            new (std::nothrow) DataDir(path.path, path.capacity_bytes, path.storage_medium));
    if (p == nullptr) {
        std::cout << "new data dir failed" << std::endl;
        return Status::InternalError("new data dir failed");
    }
    res = p->init();
    if (!res.ok()) {
        std::cout << "data_dir load failed" << std::endl;
        return Status::InternalError("data_dir load failed");
    }

    p.swap(*ret);
    return Status::OK();
}

void batch_delete_meta(const std::string& tablet_file) {
    // each line in tablet file indicate a tablet to delete, format is:
    //      data_dir,tablet_id,schema_hash
    // eg:
    //      /data1/palo.HDD,100010,11212389324
    //      /data2/palo.HDD,100010,23049230234
    std::ifstream infile(tablet_file);
    std::string line = "";
    int err_num = 0;
    int delete_num = 0;
    int total_num = 0;
    std::unordered_map<std::string, std::unique_ptr<DataDir>> dir_map;
    while (std::getline(infile, line)) {
        total_num++;
        std::vector<string> v = strings::Split(line, ",");
        if (v.size() != 3) {
            std::cout << "invalid line in tablet_file: " << line << std::endl;
            err_num++;
            continue;
        }
        // 1. get dir
        std::string dir;
        Status st = doris::io::global_local_filesystem()->canonicalize(v[0], &dir);
        if (!st.ok()) {
            std::cout << "invalid root dir in tablet_file: " << line << std::endl;
            err_num++;
            continue;
        }

        if (dir_map.find(dir) == dir_map.end()) {
            // new data dir, init it
            std::unique_ptr<DataDir> data_dir_p;
            Status st = init_data_dir(dir, &data_dir_p);
            if (!st.ok()) {
                std::cout << "invalid root path:" << FLAGS_root_path
                          << ", error: " << st.to_string() << std::endl;
                err_num++;
                continue;
            }
            dir_map[dir] = std::move(data_dir_p);
            std::cout << "get a new data dir: " << dir << std::endl;
        }
        DataDir* data_dir = dir_map[dir].get();
        if (data_dir == nullptr) {
            std::cout << "failed to get data dir: " << line << std::endl;
            err_num++;
            continue;
        }

        // 2. get tablet id/schema_hash
        int64_t tablet_id;
        if (!safe_strto64(v[1].c_str(), &tablet_id)) {
            std::cout << "invalid tablet id: " << line << std::endl;
            err_num++;
            continue;
        }
        int64_t schema_hash;
        if (!safe_strto64(v[2].c_str(), &schema_hash)) {
            std::cout << "invalid schema hash: " << line << std::endl;
            err_num++;
            continue;
        }

        Status s = TabletMetaManager::remove(data_dir, tablet_id, schema_hash);
        if (!s.ok()) {
            std::cout << "delete tablet meta failed for tablet_id:" << tablet_id
                      << ", schema_hash:" << schema_hash << ", status:" << s << std::endl;
            err_num++;
            continue;
        }

        delete_num++;
    }

    std::cout << "total: " << total_num << ", delete: " << delete_num << ", error: " << err_num
              << std::endl;
    return;
}

Status get_segment_footer(doris::io::FileReader* file_reader, SegmentFooterPB* footer) {
    // Footer := SegmentFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    std::string file_name = file_reader->path();
    uint64_t file_size = file_reader->size();
    if (file_size < 12) {
        return Status::Corruption("Bad segment file {}: file size {} < 12", file_name, file_size);
    }

    size_t bytes_read = 0;
    uint8_t fixed_buf[12];
    Slice slice(fixed_buf, 12);
    RETURN_IF_ERROR(file_reader->read_at(file_size - 12, slice, &bytes_read));

    // validate magic number
    const char* k_segment_magic = "D0R1";
    const uint32_t k_segment_magic_length = 4;
    if (memcmp(fixed_buf + 8, k_segment_magic, k_segment_magic_length) != 0) {
        return Status::Corruption("Bad segment file {}: magic number not match", file_name);
    }

    // read footer PB
    uint32_t footer_length = doris::decode_fixed32_le(fixed_buf);
    if (file_size < 12 + footer_length) {
        return Status::Corruption("Bad segment file {}: file size {} < {}", file_name, file_size,
                                  12 + footer_length);
    }
    std::string footer_buf;
    footer_buf.resize(footer_length);
    Slice slice2(footer_buf);
    RETURN_IF_ERROR(file_reader->read_at(file_size - 12 - footer_length, slice2, &bytes_read));

    // validate footer PB's checksum
    uint32_t expect_checksum = doris::decode_fixed32_le(fixed_buf + 4);
    uint32_t actual_checksum = doris::crc32c::Value(footer_buf.data(), footer_buf.size());
    if (actual_checksum != expect_checksum) {
        return Status::Corruption(
                "Bad segment file {}: footer checksum not match, actual={} vs expect={}", file_name,
                actual_checksum, expect_checksum);
    }

    // deserialize footer PB
    if (!footer->ParseFromString(footer_buf)) {
        return Status::Corruption("Bad segment file {}: failed to parse SegmentFooterPB",
                                  file_name);
    }
    return Status::OK();
}

void print_column(const ColumnMetaPB& column_pb, doris::io::FileReaderSPtr file_reader) {
    static std::string page_num_str = "page_num";
    static std::string start_str = "start";
    static std::string end_str = "end";
    static std::string size_str = "size";
    if (column_pb.has_compression()) {
        std::cout << "  compression: " << column_pb.compression();
    }
    if (column_pb.has_encoding()) {
        std::cout << "  encoding: " << column_pb.encoding();
    }
    std::cout << "  type: " << column_pb.type() << std::endl;
    for (int i = 0; i < column_pb.indexes_size(); i++) {
        auto& index_meta = column_pb.indexes(i);
        switch (index_meta.type()) {
        case doris::segment_v2::ORDINAL_INDEX: {
            auto& ordinal_index = index_meta.ordinal_index();
            std::cout << "ordinal index: " << std::endl;
            if (ordinal_index.root_page().is_root_data_page()) {
                auto& root_page = ordinal_index.root_page().root_page();
//                JsonbWriter json_writer;
//                json_writer.writeStartObject();
//                std::string page_num_str = "page_num";
//                json_writer.writeKey(page_num_str.c_str(), page_num_str.size());
//                json_writer.writeInt(1);
//                json_writer.writeKey(start_str.c_str(), start_str.size());
//                json_writer.writeInt(root_page.offset());
//                json_writer.writeKey(end_str.c_str(), end_str.size());
//                json_writer.writeInt(root_page.offset() + root_page.size());
//                json_writer.writeKey(size_str.c_str(), size_str.size());
//                json_writer.writeInt(root_page.size());
//                json_writer.writeEndObject();

                std::cout << "{\"page_num:\": 1, \"start\": " << std::to_string(root_page.offset())
                << ", \"end\": " << std::to_string(root_page.offset() + root_page.size())
                << ", \"size\": " << std::to_string(root_page.size()) << "}";
            } else {
                OlapReaderStatistics tmp_stats;
                PageReadOptions opts {
                        .use_page_cache = false,
                        .kept_in_memory = false,
                        .type = PageTypePB::INDEX_PAGE,
                        .file_reader = file_reader.get(),
                        .page_pointer = PagePointer(ordinal_index.root_page().root_page()),
                        // ordinal index page uses NO_COMPRESSION right now
                        .codec = nullptr,
                        .stats = &tmp_stats,
                        .io_ctx = doris::io::IOContext {.is_index_data = true},
                };

                // read index page
                PageHandle page_handle;
                Slice body;
                PageFooterPB footer;
                auto status = doris::segment_v2::PageIO::read_and_decompress_page(
                        opts, &page_handle, &body, &footer);
                if (!status.ok()) {
                    std::cout << "parse ordinal index err" << status.to_string() << std::endl;
                } else {
                    doris::segment_v2::IndexPageReader reader;
                    status = reader.parse(body, footer.index_page_footer());
                    if (!status.ok()) {
                        std::cout << "parse ordinal index index page err" << status.to_string()
                                  << std::endl;

                    } else {
                        auto page_count = reader.count();
                        auto& first_page_pointer = reader.get_value(0);
                        auto column_start = first_page_pointer.offset;
                        auto& last_page_pointer = reader.get_value(page_count - 1);
                        auto column_end = last_page_pointer.offset + last_page_pointer.size;

                        std::cout << "{\"page_num:\": " << std::to_string(page_count) <<
                                ", \"start\": " << std::to_string(column_start)
                                  << ", \"end\": " << std::to_string(column_end)
                                  << ", \"size\": " << std::to_string(column_end - column_start)
                                  << "}";
                    }
                }
            }
            break;
        }
        case doris::segment_v2::ZONE_MAP_INDEX: {
            auto& zone_map = index_meta.zone_map_index();
            auto& zone_map_page = zone_map.page_zone_maps();
            doris::segment_v2::IndexedColumnReader reader(file_reader, zone_map_page);
            auto status = reader.load(false, false);
            if (!status.ok()) {
                std::cout << "parse zone map index err" << status.to_string() << std::endl;
            } else {
                std::cout << "zone map index: " << std::endl;
                std::cout << reader.show_info() << std::endl;
            }
            break;
        }
        case doris::segment_v2::BITMAP_INDEX: {
            auto& bitmap = index_meta.bitmap_index();
            {
                auto& dict_index_pb =  bitmap.dict_column();
                doris::segment_v2::IndexedColumnReader reader(file_reader, dict_index_pb);
                auto status = reader.load(false, false);
                if (!status.ok()) {
                    std::cout << "parse bitmap index dict err" << status.to_string() << std::endl;
                } else {
                    std::cout << "bitmap map index dct column: " << std::endl;
                    std::cout << reader.show_info() << std::endl;
                }
            }
            {
                auto& bitmap_index_pb =  bitmap.bitmap_column();
                doris::segment_v2::IndexedColumnReader reader(file_reader, bitmap_index_pb);
                auto status = reader.load(false, false);
                if (!status.ok()) {
                    std::cout << "parse bitmap value err" << status.to_string() << std::endl;
                } else {
                    std::cout << "bitmap map index bitmap column: " << std::endl;
                    std::cout << reader.show_info() << std::endl;
                }
            }
            break;
        }
        case doris::segment_v2::BLOOM_FILTER_INDEX: {
            auto& bloom_fileter = index_meta.bloom_filter_index();
            auto& bloom_filter_pb = bloom_fileter.bloom_filter();
            doris::segment_v2::IndexedColumnReader reader(file_reader, bloom_filter_pb);
            auto status = reader.load(false, false);
            if (!status.ok()) {
                std::cout << "parse bloom filter value err" << status.to_string() << std::endl;
            } else {
                std::cout << "bloom filter index: " << std::endl;
                std::cout << reader.show_info() << std::endl;
            }
            break;
        }
        default:
            break;
        }
    }
    if (column_pb.children_columns_size() > 0) {
        for (int i = 0; i < column_pb.children_columns_size(); ++i) {
            auto& child = column_pb.children_columns(i);
            std::cout << "child column: " << std::to_string(i) << ", ros: " <<
                    std::to_string(child.num_rows()) << std::endl;
            print_column(child, file_reader);
        }
    }
    if (column_pb.sparse_columns_size() > 0) {
        for (int i = 0; i < column_pb.sparse_columns_size(); ++i) {
            auto& sparse = column_pb.sparse_columns(i);
            std::cout << "sparse column: " << std::to_string(i) << ", rows:" <<
                    std::to_string(sparse.num_rows()) << std::endl;
            print_column(sparse, file_reader);
        }
    }
}

void show_segment_footer(const std::string& file_name, const bool show_column) {
    doris::io::FileReaderSPtr file_reader;
    Status status = doris::io::global_local_filesystem()->open_file(file_name, &file_reader);
    if (!status.ok()) {
        std::cout << "open file failed: " << status << std::endl;
        return;
    }
    SegmentFooterPB footer;
    status = get_segment_footer(file_reader.get(), &footer);
    if (!status.ok()) {
        std::cout << "get footer failed: " << status.to_string() << std::endl;
        return;
    }
    std::string json_footer;
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    bool ret = json2pb::ProtoMessageToJson(footer, &json_footer, json_options);
    if (!ret) {
        std::cout << "Convert PB to json failed" << std::endl;
        return;
    }
    std::cout << json_footer << std::endl;

    if (show_column) {
        for (uint32_t ordinal = 0; ordinal < footer.columns().size(); ++ordinal) {
            const auto& column_pb = footer.columns(ordinal);
            std::cout << "column: " << std::to_string(ordinal) << std::endl;
            print_column(column_pb, file_reader);
        }
    }
    return;
}



int main(int argc, char** argv) {
    std::string usage = get_usage(argv[0]);
    gflags::SetUsageMessage(usage);
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_operation == "show_meta") {
        show_meta();
    } else if (FLAGS_operation == "batch_delete_meta") {
        std::string tablet_file;
        Status st =
                doris::io::global_local_filesystem()->canonicalize(FLAGS_tablet_file, &tablet_file);
        if (!st.ok()) {
            std::cout << "invalid tablet file: " << FLAGS_tablet_file
                      << ", error: " << st.to_string() << std::endl;
            return -1;
        }

        batch_delete_meta(tablet_file);
    } else if (FLAGS_operation == "show_segment_footer") {
        if (FLAGS_file == "") {
            std::cout << "no file flag for show dict" << std::endl;
            return -1;
        }
        show_segment_footer(FLAGS_file, FLAGS_show_column);
    } else {
        // operations that need root path should be written here
        std::set<std::string> valid_operations = {"get_meta", "load_meta", "delete_meta"};
        if (valid_operations.find(FLAGS_operation) == valid_operations.end()) {
            std::cout << "invalid operation:" << FLAGS_operation << std::endl;
            return -1;
        }

        std::unique_ptr<DataDir> data_dir;
        Status st = init_data_dir(FLAGS_root_path, &data_dir);
        if (!st.ok()) {
            std::cout << "invalid root path:" << FLAGS_root_path << ", error: " << st.to_string()
                      << std::endl;
            return -1;
        }

        if (FLAGS_operation == "get_meta") {
            get_meta(data_dir.get());
        } else if (FLAGS_operation == "load_meta") {
            load_meta(data_dir.get());
        } else if (FLAGS_operation == "delete_meta") {
            delete_meta(data_dir.get());
        } else {
            std::cout << "invalid operation: " << FLAGS_operation << "\n" << usage << std::endl;
            return -1;
        }
    }
    gflags::ShutDownCommandLineFlags();
    return 0;
}
