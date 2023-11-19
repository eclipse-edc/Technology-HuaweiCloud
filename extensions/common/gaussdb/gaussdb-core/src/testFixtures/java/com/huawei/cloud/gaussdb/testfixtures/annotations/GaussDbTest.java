package com.huawei.cloud.gaussdb.testfixtures.annotations;

import org.eclipse.edc.junit.annotations.IntegrationTest;
import org.junit.jupiter.api.Tag;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for End to End integration testing using an actual GaussDB instance.
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@IntegrationTest
@Tag("GaussDbTest")
public @interface GaussDbTest {
}
