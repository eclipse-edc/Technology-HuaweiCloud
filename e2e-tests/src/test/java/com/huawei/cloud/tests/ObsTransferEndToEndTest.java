package com.huawei.cloud.tests;

import com.huawei.cloud.fixtures.HuaweiParticipant;
import org.eclipse.edc.junit.annotations.EndToEndTest;
import org.eclipse.edc.junit.extensions.EdcClassRuntimesExtension;
import org.eclipse.edc.junit.extensions.EdcExtension;
import org.eclipse.edc.junit.extensions.EdcRuntimeExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@EndToEndTest
@ExtendWith(EdcExtension.class)
public class ObsTransferEndToEndTest {
    protected static final HuaweiParticipant CONSUMER = HuaweiParticipant.Builder.newInstance()
            .name("consumer")
            .id("consumer")
            .apiKey("password")
            .build();
    protected static final HuaweiParticipant PROVIDER = HuaweiParticipant.Builder.newInstance()
            .name("provider")
            .id("provider")
            .apiKey("password")
            .build();
    @RegisterExtension
    static EdcClassRuntimesExtension runtimes = new EdcClassRuntimesExtension(
            new EdcRuntimeExtension(
                    ":launchers",
                    "consumer",
                    CONSUMER.controlPlaneConfiguration()
            ),
            new EdcRuntimeExtension(
                    ":launchers",
                    "provider",
                    PROVIDER.controlPlaneConfiguration()
            )
    );

    @Test
    void foo() {
    }


}
