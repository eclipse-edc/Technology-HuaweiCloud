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

package com.huaweicloud.provision.obs;

import com.huawei.cloud.obs.ObsBucketSchema;
import com.huawei.cloud.obs.ObsSecretToken;
import com.huawei.cloud.obs.OtcTest;
import com.huawei.cloud.obs.TestFunctions;
import com.huaweicloud.sdk.iam.v3.IamClient;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.internal.ObsConstraint;
import org.eclipse.edc.boot.vault.InMemoryVault;
import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionResource;
import org.eclipse.edc.json.JacksonTypeManager;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

@OtcTest
public class ObsProvisionerOtcTest extends ObsProvisionerTestBase {

    private static final String OBS_OTC_CLOUD_URL = "https://obs.eu-de.otc.t-systems.com";
    private static final String IAM_OTC_CLOUD_URL = "https://iam.eu-de.otc.t-systems.com";
    private ObsClient obsClient;
    private final Vault vault = new InMemoryVault(mock(), null);
    private final TypeManager typeManager = new JacksonTypeManager();

    @Test
    void provisionWithPermission() throws ExecutionException, InterruptedException {
        var objectName = "objName";
        var definition = ProvisionResource.Builder.newInstance()
                .dataAddress(DataAddress.Builder.newInstance().type(ObsBucketSchema.TYPE)
                        .property(ObsBucketSchema.BUCKET_NAME, bucketName)
                        .build())
                .flowId("test")
                .id(UUID.randomUUID().toString())
                .build();
        var result = provisioner.provision(definition);

        assertThat(result.get().succeeded()).isTrue();

        var newClient = getObsClient(typeManager.readValue(vault.resolveSecret(result.get().getContent().getDataAddress().getKeyName()), ObsSecretToken.class));

        assertThatThrownBy(() -> newClient.listObjects(bucketName))
                .isInstanceOf(ObsException.class)
                .satisfies(e -> {
                    var obs = (ObsException) e;
                    assertThat(obs.getXmlMessage()).contains("Access Denied");
                });

        putObject(newClient, bucketName, objectName);

        var objects = obsClient.listObjects(bucketName);
        assertThat(objects.getBucketName()).isEqualTo(bucketName);

        assertThat(objects.getObjects())
                .hasSize(1)
                .satisfiesOnlyOnce(obsObject -> assertThat(obsObject.getObjectKey()).isEqualTo(objectName));

    }

    @Override
    protected ObsClient getObsClient() {
        if (obsClient == null) {
            obsClient = TestFunctions.createClient(OBS_OTC_CLOUD_URL);
        }
        return obsClient;
    }

    @Override
    protected ObsClient getObsClient(ObsSecretToken token) {
        return TestFunctions.createClient(token, OBS_OTC_CLOUD_URL);
    }

    @Override
    protected IamClient getIamClient() {
        var accessKey = System.getenv(ObsConstraint.ACCESS_KEY_ENV_VAR).trim();
        var secretKey = System.getenv(ObsConstraint.SECRET_KEY_ENV_VAR).trim();
        return TestFunctions.createIamClient(accessKey, secretKey, IAM_OTC_CLOUD_URL);
    }

    protected ObsClient getObsClient(String ak, String sk) {
        if (obsClient == null) {
            obsClient = TestFunctions.createClient(ak, sk, OBS_OTC_CLOUD_URL);
        }
        return obsClient;
    }


}
