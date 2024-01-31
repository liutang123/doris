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

package org.apache.doris.metric;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.monitor.jvm.JvmStats;
import org.apache.doris.system.Frontend;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MTJsonMetricVisitor extends MetricVisitor {
    private static final Gson gson = new Gson();

    private final List<MetricObject> metrics = new ArrayList<>();

    static class MetricObject {
        private String type;
        private String metric;
        private Object value;
        private Map<String, String> tags = new HashMap<>();

        MetricObject addTag(String key, String value) {
            this.tags.put(key, value);
            return this;
        }

        MetricObject addTags(Map<String, String> tags) {
            this.tags.putAll(tags);
            return this;
        }
    }

    @Override
    public void visitJvm(JvmStats jvmStats) {
        addGauge("palo.fe.heap.max", jvmStats.getMem().getHeapCommitted().getBytes());
        addGauge("palo.fe.heap.used", jvmStats.getMem().getHeapUsed().getBytes());
        int gcCount = jvmStats.getGc().getCollectors().length;
        if (gcCount == 1) {
            Iterator<JvmStats.GarbageCollector> iter = jvmStats.getGc().iterator();
            if (iter.hasNext()) {
                JvmStats.GarbageCollector gc = iter.next();
                addGauge("palo.fe.gc.count", gc.getCollectionCount());
                addGauge("palo.fe.gc.time", gc.getCollectionTime().millis());
            }
        } else if (gcCount > 1) {
            for (JvmStats.GarbageCollector gc : jvmStats.getGc()) {
                addGauge("palo.fe.gc.count." + gc.getName(), gc.getCollectionCount());
                addGauge("palo.fe.gc.time." + gc.getName(), gc.getCollectionTime().millis());
            }
        }
    }

    @Override
    public void visit(String prefix, @SuppressWarnings("rawtypes") Metric metric) {
        // todo: should use prefix or not?
        MetricObject object = addMetric(metric.getType().name(), metric.getName(), metric.getValue());
        List<MetricLabel> labels = metric.getLabels();
        for (MetricLabel metricLabel : labels) {
            object.addTag(metricLabel.getKey(), metricLabel.getValue());
        }

        // Return metric JSON data in getNodeInfo().
    }

    @Override
    public void visitHistogram(String prefix, String name, Histogram histogram) {
        // todo: should use prefix or not?
        // part.part.part.k1=v1.k2=v2
        List<String> names = new ArrayList<>();
        Map<String, String> tags = new HashMap<>();
        for (String part : name.split("\\.")) {
            String[] kv = part.split("=");
            if (kv.length == 1) {
                names.add(kv[0]);
            } else if (kv.length == 2) {
                tags.put(kv[0], kv[1]);
            }
        }
        final String fullName = String.join("_", names);
        Snapshot snapshot = histogram.getSnapshot();
        addGauge(fullName, "count", histogram.getCount()).addTags(tags);
        addGauge(fullName, "max", snapshot.getMax()).addTags(tags);
        addGauge(fullName, "min", snapshot.getMin()).addTags(tags);
        addGauge(fullName, "avg", snapshot.getMean()).addTags(tags);
        addGauge(fullName, "tp50", snapshot.getMedian()).addTags(tags);
        addGauge(fullName, "tp75", snapshot.get75thPercentile()).addTags(tags);
        addGauge(fullName, "tp95", snapshot.get95thPercentile()).addTags(tags);
        addGauge(fullName, "tp99", snapshot.get99thPercentile()).addTags(tags);
        addGauge(fullName, "tp999", snapshot.get999thPercentile()).addTags(tags);

        // Return metric JSON data in getNodeInfo().
    }

    @Override
    public void getNodeInfo() {
        addGauge("ready", Env.getCurrentEnv().isReady() ? 1 : 0);
        addGauge("readable", Env.getCurrentEnv().canRead() ? 1 : 0);
        addGauge("master", Env.getCurrentEnv().isMaster() ? 1 : 0);
        List<Frontend> fe = Env.getCurrentEnv().getFrontends(null);
        long beTotal = Env.getCurrentSystemInfo().getAllBackendIds(false).size();
        long beAlive = Env.getCurrentSystemInfo().getAllBackendIds(true).size();
        long beDecommissioned = Env.getCurrentSystemInfo().getDecommissionedBackendIds().size();
        List<FsBroker> broker = Env.getCurrentEnv().getBrokerMgr().getAllBrokers();
        addGauge("fe_node", "total", fe.size());
        addGauge("fe_node", "dead", fe.stream().filter(f -> !f.isAlive()).count());
        addGauge("be_node", "total", beTotal);
        addGauge("be_node", "dead", beTotal - beAlive);
        addGauge("be_node", "decommissioned", beDecommissioned);
        addGauge("broker_node", "total", broker.size());
        addGauge("broker_node", "dead", broker.stream().filter(b -> !b.isAlive).count());

        sb.append(gson.toJson(metrics));
    }

    private MetricObject addGauge(String metric, String type, Object value) {
        return addGauge(metric, value).addTag("type", type);
    }

    private MetricObject addGauge(String metric, Object value) {
        return addMetric("GAUGE", metric, value);
    }

    private MetricObject addCounter(String metric, Object value) {
        return addMetric("COUNTER", metric, value);
    }

    private MetricObject addMetric(String type, String metric, Object value) {
        MetricObject object = new MetricObject();
        this.metrics.add(object);
        object.type = type;
        object.metric = FE_PREFIX + metric;
        object.value = value;
        return object;
    }
}
