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

package org.apache.doris.catalog.authorizer.ranger.dlc;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.authorizer.ranger.RangerAccessController;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AuthorizationException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.DataMaskPolicy;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.RangerDataMaskPolicy;
import org.apache.doris.policy.DataMaskType;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class RangerDlcAccessController extends RangerAccessController {
    private static final Logger LOG = LogManager.getLogger(RangerDlcAccessController.class);
    private final RangerDlcPlugin dlcPlugin;

    public RangerDlcAccessController(Map<String, String> properties) {
        String serviceName = properties.get("ranger.service.name");
        dlcPlugin = new RangerDlcPlugin(serviceName);
    }

    private RangerAccessRequestImpl createAccessRequest(UserIdentity currentUser, RangerAccessResourceImpl resource,
                                                        DlcAccessType dlcAccessType) {
        String groupInfo = Env.getCurrentEnv().getAuth().getCamGroups(currentUser.getQualifiedUser()).trim();
        Set<String> groupIds = new HashSet<>();
        String accessType = dlcAccessType.name().toLowerCase();
        if (!Strings.isNullOrEmpty(groupInfo)) {
            groupIds.addAll(Arrays.asList(groupInfo.split(",")));
        }
        return new RangerDlcAccessRequest(resource, groupIds, accessType);
    }

    /**
     * don't use this method
     */
    @Override
    protected RangerAccessRequestImpl createRequest(UserIdentity currentUser) {
        return new RangerAccessRequestImpl();
    }

    private void checkPrivileges(UserIdentity currentUser, DlcAccessType accessType,
            List<RangerDlcResource> resources) throws AuthorizationException {
        List<RangerAccessRequest> requests = new ArrayList<>();
        for (RangerDlcResource resource : resources) {
            requests.add(createAccessRequest(currentUser, resource, accessType));
        }

        Collection<RangerAccessResult> results = dlcPlugin.isAccessAllowed(requests);
        checkRequestResults(results, accessType.name());
    }

    private boolean checkPrivilege(UserIdentity currentUser, DlcAccessType accessType,
            RangerDlcResource resource) {
        // show privilege same with dlc
        if (accessType == DlcAccessType.SHOW) {
            return true;
        }
        RangerAccessRequestImpl request = createAccessRequest(currentUser, resource, accessType);
        if (LOG.isDebugEnabled()) {
            LOG.debug("request : {}, resource : {}, user {}", request, resource, currentUser);
        }
        RangerAccessResult result = dlcPlugin.isAccessAllowed(request);
        if (LOG.isDebugEnabled()) {
            LOG.debug("access result : {}", result);
        }
        return checkRequestResult(request, result, accessType.name());
    }

    private DlcAccessType toAccessType(PrivPredicate predicate) {
        if (predicate == PrivPredicate.SHOW) {
            return DlcAccessType.SHOW;
        } else if (predicate == PrivPredicate.SELECT) {
            return DlcAccessType.SELECT;
        } else if (predicate == PrivPredicate.LOAD) {
            return DlcAccessType.INSERT;
        } else if (predicate == PrivPredicate.CREATE) {
            return DlcAccessType.CREATE;
        } else if (predicate == PrivPredicate.DROP) {
            return DlcAccessType.DROP;
        } else {
            return DlcAccessType.NONE;
        }
    }

    @Override
    public boolean checkGlobalPriv(UserIdentity currentUser, PrivPredicate wanted) {
        return Env.getCurrentEnv().getAccessManager().getAccessControllerOrDefault(
                InternalCatalog.INTERNAL_CATALOG_NAME).checkGlobalPriv(currentUser, wanted);
    }

    @Override
    public boolean checkCtlPriv(UserIdentity currentUser, String ctl, PrivPredicate wanted) {
        return checkPrivilege(currentUser, toAccessType(wanted), createResource(ctl));
    }

    @Override
    public boolean checkDbPriv(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted) {
        if (checkCtlPriv(currentUser, ctl, wanted)) {
            return true;
        }
        return checkPrivilege(currentUser, toAccessType(wanted), createResource(ctl, db));
    }

    @Override
    public boolean checkTblPriv(UserIdentity currentUser, String ctl, String db, String tbl, PrivPredicate wanted) {
        if (checkDbPriv(currentUser, ctl, db, wanted)) {
            return true;
        }
        return checkPrivilege(currentUser, toAccessType(wanted), createResource(ctl, db, tbl));
    }

    @Override
    public void checkColsPriv(UserIdentity currentUser, String ctl, String db, String tbl, Set<String> cols,
            PrivPredicate wanted) throws AuthorizationException {
        if (checkTblPriv(currentUser, ctl, db, tbl, wanted)) {
            return;
        }
        List<RangerDlcResource> resources = new ArrayList<>();
        for (String col : cols) {
            resources.add(createResource(ctl, db, tbl, col));
        }
        checkPrivileges(currentUser, toAccessType(wanted), resources);
    }

    @Override
    public boolean checkResourcePriv(UserIdentity currentUser, String resourceName, PrivPredicate wanted) {
        return false;
    }

    @Override
    public boolean checkWorkloadGroupPriv(UserIdentity currentUser, String workloadGroupName, PrivPredicate wanted) {
        return false;
    }

    protected RangerDlcResource createResource(String ctl) {
        return RangerDlcResource.of(ctl);
    }

    protected RangerDlcResource createResource(String ctl, String db) {
        return RangerDlcResource.of(ctl, ClusterNamespace.getNameFromFullName(db));
    }

    @Override
    protected RangerDlcResource createResource(String ctl, String db, String tbl) {
        return RangerDlcResource.of(ctl, ClusterNamespace.getNameFromFullName(db), tbl);
    }

    @Override
    protected RangerDlcResource createResource(String ctl, String db, String tbl, String col) {
        return RangerDlcResource.of(ctl, ClusterNamespace.getNameFromFullName(db), tbl, col);
    }

    @Override
    protected RangerBasePlugin getPlugin() {
        return dlcPlugin;
    }

    @Override
    protected RangerAccessResultProcessor getAccessResultProcessor() {
        return null;
    }

    @Override
    public Optional<DataMaskPolicy> evalDataMaskPolicy(UserIdentity currentUser, String ctl, String db, String tbl,
                                                       String col) {
        RangerAccessResourceImpl resource = createResource(ctl, db, tbl, col);
        RangerAccessRequestImpl request = createAccessRequest(currentUser, resource, DlcAccessType.SELECT);
        if (LOG.isDebugEnabled()) {
            LOG.debug("ranger request: {}", request);
        }
        RangerAccessResult policy = dlcPlugin.evalDataMaskPolicies(request, getAccessResultProcessor());
        if (LOG.isDebugEnabled()) {
            LOG.debug("ranger response: {}", policy);
        }
        if (policy == null) {
            return Optional.empty();
        }
        String maskType = policy.getMaskType();
        if (StringUtils.isEmpty(maskType)) {
            return Optional.empty();
        }
        DataMaskType type = DataMaskType.valueOf(maskType);
        String maskTypeDef;
        switch (type) {
            case MASK_SHOW_LAST_4:
            case MASK_SHOW_FIRST_4:
            case MASK_HASH:
            case MASK_DATE_SHOW_YEAR:
            case MASK_NULL:
            case MASK:
                maskTypeDef = type.getTransformer();
                break;
            default:
                return Optional.empty();
        }
        return Optional.of(new RangerDataMaskPolicy(currentUser, ctl, db, tbl, col, policy.getPolicyId(),
            policy.getPolicyVersion(), type, maskTypeDef.replace("{col}", col)));
    }

    @Override
    public void cleanup() {
        if (dlcPlugin != null) {
            dlcPlugin.cleanup();
            if (LOG.isDebugEnabled()) {
                LOG.debug("clean up dlc ranger plugin successfully.");
            }
        }
    }
}
