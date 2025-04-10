/*
 *  Copyright (c) 2024 Huawei Technologies
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Huawei Technologies - initial API and implementation
 *
 */

package com.huawei.cloud.fixtures;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.eclipse.edc.connector.controlplane.test.system.utils.LazySupplier;
import org.eclipse.edc.connector.controlplane.test.system.utils.Participant;
import org.eclipse.edc.spi.system.configuration.Config;
import org.eclipse.edc.spi.system.configuration.ConfigFactory;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.eclipse.edc.boot.BootServicesExtension.PARTICIPANT_ID;
import static org.eclipse.edc.util.io.Ports.getFreePort;


public class HuaweiParticipant extends Participant {

    private static final String IAM_OTC_CLOUD_URL = "https://iam.eu-de.otc.t-systems.com";

    private String apiKey;
    private final LazySupplier<URI> controlEndpoint = new LazySupplier<>(() -> URI.create("http://localhost:" + getFreePort() + "/control"));

    public URI getControlEndpoint() {
        return controlEndpoint.get();
    }

    public Config controlPlaneConfig() {
        var settings = (Map<String, String>) new HashMap<String, String>() {
            {
                put(PARTICIPANT_ID, id);
                put("edc.api.auth.key", apiKey);
                put("web.http.port", String.valueOf(getFreePort()));
                put("web.http.path", "/api");
                put("web.http.protocol.port", String.valueOf(controlPlaneProtocol.get().getPort()));
                put("web.http.protocol.path", controlPlaneProtocol.get().getPath());
                put("web.http.management.port", String.valueOf(controlPlaneManagement.get().getPort()));
                put("web.http.management.path", controlPlaneManagement.get().getPath());
                put("web.http.control.port", String.valueOf(controlEndpoint.get().getPort()));
                put("web.http.control.path", controlEndpoint.get().getPath());
                put("web.http.public.path", "/public");
                put("web.http.public.port", String.valueOf(getFreePort()));
                put("edc.dsp.callback.address", controlPlaneProtocol.get().toString());
                put("edc.connector.name", name);
                put("edc.hostname", name);
                put("edc.component.id", "connector-test");
                put("edc.dataplane.token.validation.endpoint", "http://token-validation.com");
                put("edc.dpf.selector.url", "http://does-this-matter.com");
                put("edc.huawei.iam.endpoint", IAM_OTC_CLOUD_URL);
                put("edc.transfer.proxy.token.verifier.publickey.alias", "publickey");
                put("edc.transfer.proxy.token.signer.privatekey.alias", "privatekey");
            }
        };

        return ConfigFactory.fromMap(settings);
    }

    public static final class Builder extends Participant.Builder<HuaweiParticipant, Builder> {

        private Builder() {
            super(new HuaweiParticipant());
        }

        @JsonCreator
        public static Builder newInstance() {
            return new Builder();
        }

        public Builder apiKey(String apiKey) {
            this.participant.apiKey = apiKey;
            return this;
        }

        @Override
        public HuaweiParticipant build() {
            participant.enrichManagementRequest = request -> request.header("X-Api-Key", participant.apiKey);
            return super.build();
        }
    }
}
