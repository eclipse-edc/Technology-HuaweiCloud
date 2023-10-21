package com.huawei.cloud.obs;

import com.obs.services.BasicObsCredentialsProvider;
import com.obs.services.OBSCredentialsProviderChain;
import org.eclipse.edc.junit.extensions.DependencyInjectionExtension;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.system.configuration.Config;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;

import static com.huawei.cloud.obs.ObsCoreExtension.HUAWEI_ACCESS_KEY;
import static com.huawei.cloud.obs.ObsCoreExtension.HUAWEI_IAM_ENDPOINT;
import static com.huawei.cloud.obs.ObsCoreExtension.HUAWEI_SECRET_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(DependencyInjectionExtension.class)
public class ObsCoreExtensionTest {
    
    private final Vault vault = mock();
    private ServiceExtensionContext context;

    @BeforeEach
    void setup(ServiceExtensionContext context) {
        context.registerService(Vault.class, vault);
        this.context = spy(context);
    }

    @Test
    void initialize(ObsCoreExtension extension) {
        var iamEndpoint = "http://iam";
        var cfg = mock(Config.class);
        var accessKey = "ak";
        var secretKey = "sk";

        when(context.getConfig()).thenReturn(cfg);
        when(context.getSetting(HUAWEI_ACCESS_KEY, HUAWEI_ACCESS_KEY)).thenReturn(accessKey);
        when(context.getSetting(HUAWEI_SECRET_KEY, HUAWEI_SECRET_KEY)).thenReturn(secretKey);

        when(cfg.getString(HUAWEI_IAM_ENDPOINT)).thenReturn(iamEndpoint);

        when(vault.resolveSecret(accessKey)).thenReturn(accessKey);
        when(vault.resolveSecret(secretKey)).thenReturn(secretKey);

        var captor = ArgumentCaptor.forClass(ObsClientProviderImpl.class);

        extension.initialize(context);

        verify(context).registerService(any(), captor.capture());

        var clientProviderCfg = captor.getValue().getConfiguration();

        assertThat(clientProviderCfg.getIamEndpoint()).isEqualTo(iamEndpoint);
        assertThat(clientProviderCfg.getCredentialsProvider()).isInstanceOf(BasicObsCredentialsProvider.class)
                .satisfies(provider -> {
                    assertThat(provider.getSecurityKey().getAccessKey()).isEqualTo(accessKey);
                    assertThat(provider.getSecurityKey().getSecretKey()).isEqualTo(secretKey);
                });
        assertThat(clientProviderCfg.getIamEndpoint()).isEqualTo(iamEndpoint);
    }

    @Test
    void initialize_defaultCredentials(ObsCoreExtension extension) {
        var iamEndpoint = "http://iam";
        var cfg = mock(Config.class);

        when(context.getConfig()).thenReturn(cfg);
        when(cfg.getString(HUAWEI_IAM_ENDPOINT)).thenReturn(iamEndpoint);


        var captor = ArgumentCaptor.forClass(ObsClientProviderImpl.class);

        extension.initialize(context);

        verify(context).registerService(any(), captor.capture());

        var clientProviderCfg = captor.getValue().getConfiguration();

        assertThat(clientProviderCfg.getIamEndpoint()).isEqualTo(iamEndpoint);
        assertThat(clientProviderCfg.getCredentialsProvider()).isInstanceOf(OBSCredentialsProviderChain.class);
    }
}
