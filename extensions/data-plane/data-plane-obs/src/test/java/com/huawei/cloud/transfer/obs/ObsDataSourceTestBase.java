package com.huawei.cloud.transfer.obs;

import com.obs.services.ObsClient;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.junit.testfixtures.TestUtils.getFileFromResourceName;

@SuppressWarnings("ALL") // try-with-resources is not needed here
abstract class ObsDataSourceTestBase {

    private String bucketName;

    @BeforeEach
    void setup(TestInfo testInfo) {
        bucketName = createBucket(testInfo);
    }

    @Test
    void openPartStream_withPrefix() {
        var file = getFileFromResourceName("test-file-upload.txt");
        putFile(file, bucketName, "file1.txt");

        var source = ObsDataSource.Builder.newInstance()
                .client(getClient())
                .bucketName(bucketName)
                .keyPrefix("f")
                .build();

        var str = source.openPartStream();
        assertThat(str.succeeded()).isTrue();
        assertThat(str.getContent()).hasSize(1)
                .map(part -> (ObsDataSource.ObsPart) part)
                .allSatisfy(obsPart -> {
                    assertThat(obsPart.name()).isEqualTo("file1.txt");
                    assertThat(obsPart.size()).isNotZero();
                });
    }

    @Test
    void openPartStream_withPrefix_noMatch() {
        var file = getFileFromResourceName("test-file-upload.txt");
        putFile(file, bucketName, "file1.txt");

        StreamResult<Stream<DataSource.Part>> str;

        var source = ObsDataSource.Builder.newInstance()
                .client(getClient())
                .bucketName(bucketName)
                .keyPrefix("xyz")
                .build();

        str = source.openPartStream();
        assertThat(str.succeeded()).isFalse();
        assertThat(str.getContent()).isNull();
    }

    @Test
    void openPartStream_withPrefix_emptyBucket() {

        StreamResult<Stream<DataSource.Part>> str;

        var source = ObsDataSource.Builder.newInstance()
                .client(getClient())
                .bucketName(bucketName)
                .keyPrefix("f")
                .build();

        str = source.openPartStream();
        assertThat(str.succeeded()).isFalse();
        assertThat(str.getContent()).isNull();
    }

    @Test
    void openPartStream_noPrefix() {

        var file = getFileFromResourceName("test-file-upload.txt");
        putFile(file, bucketName, "file1.txt");

        StreamResult<Stream<DataSource.Part>> str;

        var source = ObsDataSource.Builder.newInstance()
                .client(getClient())
                .bucketName(bucketName)
                .build();

        str = source.openPartStream();
        assertThat(str.succeeded()).isTrue();
        assertThat(str.getContent()).hasSize(1);
    }

    @Test
    void openPartStream_noPrefix_empty() {
        StreamResult<Stream<DataSource.Part>> str;

        var source = ObsDataSource.Builder.newInstance()
                .client(getClient())
                .bucketName(bucketName)
                .build();

        str = source.openPartStream();
        assertThat(str.succeeded()).isFalse();
        assertThat(str.getContent()).isNull();
    }

    abstract ObsClient getClient();

    @NotNull
    protected abstract String createBucket(TestInfo testInfo);


    private void putFile(File file, String bucketName, String objectKey) {
        var c = getClient();
        c.putObject(bucketName, objectKey, file);
    }
}