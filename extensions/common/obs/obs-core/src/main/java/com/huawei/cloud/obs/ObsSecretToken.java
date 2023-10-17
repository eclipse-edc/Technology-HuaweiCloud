package com.huawei.cloud.obs;

import java.util.Objects;

public record ObsSecretToken(String ak, String sk) {

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ObsSecretToken that = (ObsSecretToken) o;
        return Objects.equals(ak, that.ak) && Objects.equals(sk, that.sk);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ak, sk);
    }
}
