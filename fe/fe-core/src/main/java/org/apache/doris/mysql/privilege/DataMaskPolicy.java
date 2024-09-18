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

package org.apache.doris.mysql.privilege;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;

public interface DataMaskPolicy {
    String getMaskTypeDef();

    String getPolicyIdent();

    Expression parseMaskTypeDef(NereidsParser parser, Slot slot);

    default String getDataTypeDefaultValue(Slot slot) {
        Type dataType = slot.getDataType().toCatalogDataType();
        if (dataType instanceof ScalarType) {
            switch (dataType.getPrimitiveType()) {
                case BOOLEAN:
                    return "false";
                case FLOAT:
                case DOUBLE:
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                case LARGEINT:
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                case DECIMAL256:
                    return "0";
                case CHAR:
                case VARCHAR:
                case STRING:
                case BINARY:
                    return "";
                case DATEV2:
                case DATE:
                    return "1970-01-01";
                case TIME:
                case TIMEV2:
                    return "00:00:00";
                case DATETIME:
                case DATETIMEV2:
                    return "1970-01-01 08:00:00";
                case IPV4:
                    return "0.0.0.0";
                case ARRAY:
                    return "[]";
                case MAP:
                case JSONB:
                case STRUCT:
                    return "{}";
                default:
                    return "NULL";
            }
        }
        return "NULL";
    }
}
