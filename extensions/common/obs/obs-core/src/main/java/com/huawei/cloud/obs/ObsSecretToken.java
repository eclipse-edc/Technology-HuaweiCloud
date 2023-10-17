package com.huawei.cloud.obs;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.eclipse.edc.connector.transfer.spi.types.SecretToken;

import java.util.Objects;

@JsonTypeName("dataspaceconnector:obssecrettoken")
public record ObsSecretToken(@JsonProperty(value = "ak") String ak,
                             @JsonProperty(value = "sk") String sk,
                             @JsonProperty(value = "securitytoken") String securityToken,
                             @JsonProperty("expiration") Long expiration) implements SecretToken {

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ObsSecretToken that = (ObsSecretToken) o;
        return Objects.equals(ak, that.ak) && Objects.equals(sk, that.sk) && Objects.equals(securityToken, that.securityToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ak, sk, securityToken);
    }

    @Override
    public long getExpiration() {
        return expiration != null ? expiration : 0;
    }
}
