package com.huawei.cloud.transfer.obs;

import com.huawei.cloud.obs.ObsBucketSchema;
import com.huawei.cloud.obs.ObsClientProvider;
import com.huawei.cloud.transfer.obs.validation.ObsDataAddressValidationRule;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSourceFactory;
import org.eclipse.edc.connector.dataplane.util.validation.ValidationRule;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowRequest;
import org.jetbrains.annotations.NotNull;

import static com.huawei.cloud.obs.ObsBucketSchema.BUCKET_NAME;
import static com.huawei.cloud.obs.ObsBucketSchema.KEY_PREFIX;

public class ObsDataSourceFactory extends ObsFactory implements DataSourceFactory {

    private final ValidationRule<DataAddress> validation = new ObsDataAddressValidationRule();

    public ObsDataSourceFactory(Vault vault, TypeManager typeManager, ObsClientProvider clientProvider) {
        super(vault, typeManager, clientProvider);
    }

    @Override
    public boolean canHandle(DataFlowRequest request) {
        return ObsBucketSchema.TYPE.equals(request.getSourceDataAddress().getType());
    }

    @Override
    public DataSource createSource(DataFlowRequest request) {
        var validationResult = validateRequest(request);
        if (validationResult.failed()) {
            throw new EdcException(String.join(", ", validationResult.getFailureMessages()));
        }

        var source = request.getSourceDataAddress();

        return ObsDataSource.Builder.newInstance()
                .bucketName(source.getStringProperty(BUCKET_NAME))
                .client(createObsClient(source))
                .keyPrefix(source.getStringProperty(KEY_PREFIX, null))
                .build();
    }


    @Override
    public @NotNull Result<Void> validateRequest(DataFlowRequest request) {
        var source = request.getSourceDataAddress();

        return validation.apply(source).map(it -> null);
    }

}
