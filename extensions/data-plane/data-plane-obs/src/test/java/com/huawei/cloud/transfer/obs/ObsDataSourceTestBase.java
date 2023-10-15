/*
 *
 *   Copyright (c) 2023 Bayerische Motoren Werke Aktiengesellschaft
 *
 *   See the NOTICE file(s) distributed with this work for additional
 *   information regarding copyright ownership.
 *
 *   This program and the accompanying materials are made available under the
 *   terms of the Apache License, Version 2.0 which is available at
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *   License for the specific language governing permissions and limitations
 *   under the License.
 *
 *   SPDX-License-Identifier: Apache-2.0
 *
 */

package com.huawei.cloud.transfer.obs;

import com.obs.services.ObsClient;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult;
import org.eclipse.edc.junit.annotations.EndToEndTest;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.junit.testfixtures.TestUtils.getFileFromResourceName;

@SuppressWarnings("ALL") // try-with-resources is not needed here
@EndToEndTest
@Testcontainers
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