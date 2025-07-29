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

package org.apache.doris.datasource.setats.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.property.constants.SetatsProperties;
import org.apache.doris.datasource.setats.SetatsExternalTable;
import org.apache.doris.thrift.TFileAttributes;

import com.tencent.oceanus.table.Table;


public class SetatsSource {
    private final SetatsExternalTable setatsExtTable;
    private final Table originTable;

    private final TupleDescriptor desc;

    public SetatsSource(TupleDescriptor desc) {
        this.desc = desc;
        this.setatsExtTable = (SetatsExternalTable) desc.getTable();
        this.originTable = setatsExtTable.getSetatsTable();
    }

    public TupleDescriptor getDesc() {
        return desc;
    }

    public Table getSetatsTable() {
        return originTable;
    }

    public TableIf getTargetTable() {
        return setatsExtTable;
    }

    public TFileAttributes getFileAttributes() throws UserException {
        return new TFileAttributes();
    }

    public ExternalCatalog getCatalog() {
        return setatsExtTable.getCatalog();
    }

    public String getFileFormat() {
        return originTable.options().getOrDefault(SetatsProperties.FILE_FORMAT, "parquet");
    }
}
