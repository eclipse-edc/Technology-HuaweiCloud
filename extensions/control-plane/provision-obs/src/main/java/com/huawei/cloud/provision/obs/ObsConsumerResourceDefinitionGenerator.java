package com.huawei.cloud.provision.obs;

import com.huawei.cloud.obs.ObsBucketSchema;
import org.eclipse.edc.connector.controlplane.transfer.spi.provision.ConsumerResourceDefinitionGenerator;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.ResourceDefinition;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcess;
import org.eclipse.edc.policy.model.Policy;
import org.jetbrains.annotations.Nullable;

import static java.util.UUID.randomUUID;

public class ObsConsumerResourceDefinitionGenerator implements ConsumerResourceDefinitionGenerator {

    @Override
    public @Nullable ResourceDefinition generate(TransferProcess dataRequest, Policy policy) {
        if (dataRequest.getDataDestination().getStringProperty(ObsBucketSchema.ENDPOINT) == null) {
            return ObsResourceDefinition.Builder.newInstance()
                    .id(randomUUID().toString())
                    .bucketName(dataRequest.getDataDestination().getStringProperty(ObsBucketSchema.BUCKET_NAME))
                    .build();
        }
        var destination = dataRequest.getDataDestination();
        var id = randomUUID().toString();

        return ObsResourceDefinition.Builder.newInstance()
                .id(id)
                .bucketName(destination.getStringProperty(ObsBucketSchema.BUCKET_NAME))
                .endpoint(destination.getStringProperty(ObsBucketSchema.ENDPOINT))
                .build();
    }



    @Override
    public boolean canGenerate(TransferProcess dataRequest, Policy policy) {
        return ObsBucketSchema.TYPE.equals(dataRequest.getDestinationType());
    }
}
