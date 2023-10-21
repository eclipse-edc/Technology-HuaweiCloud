package com.huawei.cloud.obs;

import com.obs.services.BasicObsCredentialsProvider;
import com.obs.services.IObsCredentialsProvider;
import com.obs.services.OBSCredentialsProviderChain;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.runtime.metamodel.annotation.Provides;
import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.jetbrains.annotations.NotNull;

import static java.lang.String.format;


@Extension(ObsCoreExtension.NAME)
@Provides(ObsClientProvider.class)
public class ObsCoreExtension implements ServiceExtension {

    @Setting(value = "The key of the secret where the AWS Access Key Id is stored")
    public static final String HUAWEI_ACCESS_KEY = "edc.huawei.obs.alias.ak";
    @Setting(value = "The key of the secret where the AWS Secret Access Key is stored")
    public static final String HUAWEI_SECRET_KEY = "edc.huawei.obs.alias.sk";
    @Setting(value = "If valued, the AWS clients will point to the specified endpoint")
    public static final String HUAWEI_IAM_ENDPOINT = "edc.huawei.iam.endpoint";
    protected static final String NAME = "OBS Core";
    private ObsClientProvider clientProvider;

    @Inject
    private Vault vault;

    @Inject
    private Monitor monitor;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void initialize(ServiceExtensionContext context) {

        var iamEndpoint = context.getConfig().getString(HUAWEI_IAM_ENDPOINT);

        var configuration = ObsClientProviderConfiguration.Builder.newInstance()
                .credentialsProvider(createCredentialsProvider(context))
                .iamEndpoint(iamEndpoint)
                .build();

        clientProvider = new ObsClientProviderImpl(configuration, monitor);

        context.registerService(ObsClientProvider.class, clientProvider);
    }

    @Override
    public void shutdown() {
        clientProvider.shutdown();
    }

    @NotNull
    private IObsCredentialsProvider createCredentialsProvider(ServiceExtensionContext context) {
        var accessKey = vault.resolveSecret(context.getSetting(HUAWEI_ACCESS_KEY, HUAWEI_ACCESS_KEY));
        var secretKey = vault.resolveSecret(context.getSetting(HUAWEI_SECRET_KEY, HUAWEI_SECRET_KEY));

        if (accessKey == null || secretKey == null) {
            monitor.info(format("OBS: %s and %s were not found in the vault, EnvironmentVariableObsCredentialsProvider will be used", HUAWEI_ACCESS_KEY, HUAWEI_SECRET_KEY));
            return new OBSCredentialsProviderChain();
        }

        return new BasicObsCredentialsProvider(accessKey, secretKey);
    }
}
