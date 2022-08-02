//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef DORIS_BE_UTIL_METRIC_LOG_H
#define DORIS_BE_UTIL_METRIC_LOG_H

#include <cstdint>
#include <string>
#include <utility>

#include "common/status.h"
#include "util/runtime_profile.h"

/**
 *  Used for MT metric
 */

namespace doris {

Status init_metric_log();

struct MetricLog {
    // required fields
    // ---------------
    std::string metric;
    int64_t value;

    // optional fields
    // ---------------
    std::string db_name;
    std::string table_name;
    std::string query_id;
    std::string fragment_id;
    std::string thread;
    std::string query_type;
    int64_t tablet = -1;
};

// init_metric_log() must be called before
void emit_metric_log(const MetricLog& log, int64_t emit_threshold = 0);

struct LoadsLog {
    // required fields
    // ---------------
    std::string db;
    std::string table;
    std::string label;
    std::string status;
    std::string message;

    // optional fields
    // ---------------
    std::string cluster;
    std::string user;
    std::string user_ip;

    int64_t receive_bytes = 0; // read bytes
    int64_t number_loaded_rows = 0; // loaded rows
    int64_t number_filtered_rows = 0; // filtered rows
    int64_t load_cost_ms = 0; // 耗费的时间
    int64_t load_type = 0; // load type: default stream load -> 0, spark load -> 1
};
// init_metric_log() must be called before
void emit_loads_log(const LoadsLog& load);

class XMDLog {
public:
    XMDLog(std::string logger) : _logger(std::move(logger)) {};

    XMDLog& tag(const std::string& key, const std::string& value);
    XMDLog& tag_format_k(const std::string& key, const std::string& value);
    XMDLog& tag_format_v(const std::string& key, const std::string& value);
    XMDLog& tag_format_kv(const std::string& key, const std::string& value);

    XMDLog& json(const std::string& json_str);

    void log();

private:
    std::string _logger;
    std::stringstream _buf;
};
} // namespace doris

#endif
