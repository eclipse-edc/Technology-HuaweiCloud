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

package com.huawei.cloud.provision.obs;

import com.huawei.cloud.obs.ObsSecretToken;
import com.huawei.cloud.obs.OtcTest;
import com.huawei.cloud.obs.TestFunctions;
import com.huaweicloud.sdk.iam.v3.IamClient;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.internal.ObsConstraint;
import org.eclipse.edc.policy.model.Policy;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@OtcTest
public class ObsProvisionerOtcTest extends ObsProvisionerTestBase {

    private static final String OBS_OTC_CLOUD_URL = "https://obs.eu-de.otc.t-systems.com";
    private static final String IAM_OTC_CLOUD_URL = "https://iam.eu-de.otc.t-systems.com";
    private ObsClient obsClient;

    @Test
    void provisionWithPermission() throws ExecutionException, InterruptedException {
        var objectName = "objName";
        var endpoint = "http://localhost";
        var definition = ObsResourceDefinition.Builder.newInstance()
                .endpoint(endpoint)
                .bucketName(bucketName)
                .transferProcessId(UUID.randomUUID().toString())
                .id(UUID.randomUUID().toString())
                .build();

        var result = provisioner.provision(definition, Policy.Builder.newInstance().build()).get();

        assertThat(result.succeeded()).isTrue();

        var newClient = getObsClient((ObsSecretToken) result.getContent().getSecretToken());

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
