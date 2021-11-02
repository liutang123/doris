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

package org.apache.doris.common.mt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.service.FrontendOptions;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MGWClient implements Closeable {

    private static final Logger LOG = LogManager.getLogger(MGWClient.class);

    private final static String MGW_ADD_RS_URL = "http://mip.sankuai.com/mgw/rs/add/";
    private final static String MGW_DELETE_RS_URL = "http://mip.sankuai.com/mgw/rs/delete/";
    private final static String MGW_QUERY_RS_URL = "http://mip.sankuai.com/mgw/rs/query/";
    private final static String MGW_AUTHORIZATION = "Token at1x8o50ss3gex6rdw7kvxjc7clhrtaw";
    private final static String MGW_PROTOCOL = "TCP";

    private final CloseableHttpClient client;
    private final ObjectMapper mapper;

    private final String vip;
    private final String rip;

    private MGWClient() throws UnknownHostException {
        final RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(3000)
                .setConnectTimeout(3000)
                .setSocketTimeout(3000)
                .build();

        this.client = HttpClients.custom()
                .disableCookieManagement()
                .setDefaultRequestConfig(requestConfig)
                .build();

        this.mapper = new ObjectMapper();

        this.vip = InetAddress.getByName(Config.mt_domain).getHostAddress();
        this.rip = FrontendOptions.getLocalHostAddress();
    }

    private List<String> search(int port) throws URISyntaxException {
        int[] a = Arrays.stream(this.vip.split("\\.")).mapToInt(Integer::parseInt).toArray();
        String vs = String.format("v%03d%03d%03d%03d%s%s", a[0], a[1], a[2], a[3], MGW_PROTOCOL.charAt(0), port);
        HttpGet request = new HttpGet(new URIBuilder(MGW_QUERY_RS_URL)
                .addParameter("vs", vs)
                .addParameter("valid", "enable")
                .build());
        request.setHeader("Authorization", MGW_AUTHORIZATION);
        try (CloseableHttpResponse response = client.execute(request)) {
            String body = response.getEntity() == null ? null : EntityUtils.toString(response.getEntity());
            if (StringUtils.isEmpty(body)) {
                throw new IOException("query mgw rs with empty body");
            }
            JsonNode root = mapper.readTree(body);
            if (root.get("status").asText("").equals("success")) {
                return StreamSupport.stream(root.get("data").spliterator(), false)
                        .map(node -> node.get("rip").asText())
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
            } else {
                throw new IOException(body);
            }
        } catch (IOException e) {
            LOG.error("query mgw rs failed", e);
            MTAlertDaemon.error("query mgw rs failed", e);
            return Collections.emptyList();
        }
    }

    private void operate(String url, int port) throws IOException {
        HttpPost request = new HttpPost(url);
        request.setHeader("Authorization", MGW_AUTHORIZATION);

        ObjectNode data = mapper.createObjectNode();
        data.put("vip", this.vip);
        data.put("vport", String.valueOf(port));
        data.put("rip", this.rip);
        data.put("rport", String.valueOf(port));
        data.put("protocol", MGW_PROTOCOL);
        NameValuePair param = new BasicNameValuePair("data", mapper.writeValueAsString(data));
        request.setEntity(new UrlEncodedFormEntity(Collections.singletonList(param), "UTF-8"));
        try (CloseableHttpResponse response = client.execute(request)) {
            String body = response.getEntity() == null ? null : EntityUtils.toString(response.getEntity());
            if (StringUtils.isEmpty(body)) {
                throw new IOException("operate mgw rs with empty body");
            }
            JsonNode root = mapper.readTree(body);
            if (root.get("status").asText("").equals("success")) {
                LOG.info("operate mgw rs success");
            } else {
                throw new IOException(body);
            }
        } catch (IOException e) {
            LOG.error("operate mgw rs failed", e);
            MTAlertDaemon.error("operate mgw rs failed", e);
        }
    }

    private void add(int port) throws IOException, URISyntaxException {
        LOG.info("start to add mgw rs: {}:{}", this.rip, port);
        List<String> rips = this.search(port);
        if (!rips.contains(this.rip)) {
            this.operate(MGW_ADD_RS_URL, port);
        } else {
            LOG.info("no need to add mgw rs, existed rs: {}", rips);
        }
    }

    private void delete(int port) throws IOException, URISyntaxException {
        LOG.info("start to delete mgw rs: {}:{}", this.rip, port);
        List<String> rips = this.search(port);
        if (rips.size() == 0) {
            LOG.info("try to add mgw rs because no existed rs found: {}", this.rip);
            this.operate(MGW_ADD_RS_URL, port);
        } else if (rips.size() > 1 && rips.contains(this.rip)) {
            this.operate(MGW_DELETE_RS_URL, port);
        } else {
            LOG.info("no need to delete mgw rs, existed rs: {}", rips);
        }
    }

    @Override
    public void close() throws IOException {
        this.client.close();
    }

    public static void register() {
        if (Config.mt_disable_mgw_ops) {
            LOG.info("disable mgw ops");
            return;
        }
        try (MGWClient client = new MGWClient()) {
            Env env = Env.getServingEnv();
            if (env.canRead() && !env.isMaster()) {
                client.add(Config.query_port);
            } else {
                client.delete(Config.query_port);
            }
            if (env.isReady()) {
                client.add(Config.http_port);
            } else {
                client.delete(Config.http_port);
            }
        } catch (Exception e) {
            LOG.error("register mgw error", e);
            MTAlertDaemon.error("register mgw error", e);
        }
    }

    public static void deregister() {
        if (Config.mt_disable_mgw_ops) {
            LOG.info("disable mgw ops");
            return;
        }
        try (MGWClient client = new MGWClient()) {
            client.delete(Config.query_port);
            client.delete(Config.http_port);
        } catch (Exception e) {
            LOG.error("deregister mgw error", e);
            MTAlertDaemon.error("deregister mgw error", e);
        }
    }

    public static Daemon createDaemon() {
        return new Daemon("mgw daemon", checkInterval()) {
            @Override
            protected void runOneCycle() {
                MGWClient.register();
                this.setInterval(checkInterval());
            }
        };
    }

    private static long checkInterval() {
        return 1000L * (Config.mt_check_mgw_interval_second < 0 ? 30 : Config.mt_check_mgw_interval_second);
    }

}
