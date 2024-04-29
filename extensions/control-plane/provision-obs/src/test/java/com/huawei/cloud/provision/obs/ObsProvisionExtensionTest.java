package com.huawei.cloud.provision.obs;


import org.eclipse.edc.connector.controlplane.transfer.spi.provision.ConsumerResourceDefinitionGenerator;
import org.eclipse.edc.connector.controlplane.transfer.spi.provision.ProvisionManager;
import org.eclipse.edc.connector.controlplane.transfer.spi.provision.Provisioner;
import org.eclipse.edc.connector.controlplane.transfer.spi.provision.ResourceManifestGenerator;
import org.eclipse.edc.junit.extensions.DependencyInjectionExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.types.TypeManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(DependencyInjectionExtension.class)
public class ObsProvisionExtensionTest {

    private final ProvisionManager provisionManager = mock();

    private final ResourceManifestGenerator resourceManifestGenerator = mock();

    private final TypeManager typeManager = mock();

    @BeforeEach
    void setUp(ServiceExtensionContext context) {
        context.registerService(ProvisionManager.class, provisionManager);
        context.registerService(ResourceManifestGenerator.class, resourceManifestGenerator);
        context.registerService(TypeManager.class, typeManager);
    }

    @Test
    void initialize(ServiceExtensionContext context, ObsProvisionExtension extension) {
        extension.initialize(context);

        var provisionerCaptor = ArgumentCaptor.forClass(Provisioner.class);
        verify(provisionManager).register(provisionerCaptor.capture());
        assertThat(provisionerCaptor.getValue()).isInstanceOf(ObsProvisioner.class);

        var generatorCaptor = ArgumentCaptor.forClass(ConsumerResourceDefinitionGenerator.class);
        verify(resourceManifestGenerator).registerGenerator(generatorCaptor.capture());
        assertThat(generatorCaptor.getValue()).isInstanceOf(ObsConsumerResourceDefinitionGenerator.class);

        var typesCaptor = ArgumentCaptor.forClass(Class[].class);
        verify(typeManager).registerTypes(typesCaptor.capture());
        assertThat(typesCaptor.getValue()).containsExactly(ObsProvisionedResource.class, ObsResourceDefinition.class);

    }

}
