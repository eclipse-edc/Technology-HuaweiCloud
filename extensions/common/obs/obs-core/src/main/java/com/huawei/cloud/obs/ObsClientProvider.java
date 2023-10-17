package com.huawei.cloud.obs;

import com.huaweicloud.sdk.iam.v3.IamClient;
import com.obs.services.ObsClient;
import org.eclipse.edc.runtime.metamodel.annotation.ExtensionPoint;

@ExtensionPoint
public interface ObsClientProvider {

    /**
     * Returns the client for the specified endpoint
     */
    ObsClient obsClient(String endpoint);

    /**
     * Returns the iam client
     */
    IamClient iamClient();

    /**
     * Releases resources used.
     */
    void shutdown();
}
