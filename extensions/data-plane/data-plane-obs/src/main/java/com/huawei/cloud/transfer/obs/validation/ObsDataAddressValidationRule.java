package com.huawei.cloud.transfer.obs.validation;

import org.eclipse.edc.connector.dataplane.util.validation.CompositeValidationRule;
import org.eclipse.edc.connector.dataplane.util.validation.EmptyValueValidationRule;
import org.eclipse.edc.connector.dataplane.util.validation.ValidationRule;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.types.domain.DataAddress;

import java.util.List;

import static com.huawei.cloud.obs.ObsBucketSchema.BUCKET_NAME;
import static com.huawei.cloud.obs.ObsBucketSchema.ENDPOINT;

public class ObsDataAddressValidationRule implements ValidationRule<DataAddress> {
    @Override
    public Result<Void> apply(DataAddress dataAddress) {
        var composite = new CompositeValidationRule<>(
                List.of(
                        new EmptyValueValidationRule(BUCKET_NAME),
                        new EmptyValueValidationRule(ENDPOINT)
                )
        );

        return composite.apply(dataAddress);
    }
}
