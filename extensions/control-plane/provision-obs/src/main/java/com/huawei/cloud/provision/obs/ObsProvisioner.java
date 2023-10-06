/*
 *
 *   Copyright (c) 2023 Bayerische Motoren Werke Aktiengesellschaft
 *
 *   See the NOTICE file(s) distributed with this work for additional
 *   information regarding copyright ownership.
 *
 *   This program and the accompanying materials are made available under the
 *   terms of the Apache License, Version 2.0 which is available at
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *   License for the specific language governing permissions and limitations
 *   under the License.
 *
 *   SPDX-License-Identifier: Apache-2.0
 *
 */

package com.huawei.cloud.provision.obs;

import org.eclipse.edc.connector.transfer.spi.provision.Provisioner;
import org.eclipse.edc.connector.transfer.spi.types.DeprovisionedResource;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionResponse;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionedResource;
import org.eclipse.edc.connector.transfer.spi.types.ResourceDefinition;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.response.StatusResult;

import java.util.concurrent.CompletableFuture;

public class ObsProvisioner implements Provisioner<ObsResourceDefinition, ObsProvisionedResource> {
    @Override
    public boolean canProvision(ResourceDefinition resourceDefinition) {
        return false;
    }

    @Override
    public boolean canDeprovision(ProvisionedResource resourceDefinition) {
        return false;
    }

    @Override
    public CompletableFuture<StatusResult<ProvisionResponse>> provision(ObsResourceDefinition resourceDefinition, Policy policy) {
        return null;
    }

    @Override
    public CompletableFuture<StatusResult<DeprovisionedResource>> deprovision(ObsProvisionedResource provisionedResource, Policy policy) {
        return null;
    }
}
