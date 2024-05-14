/*
 *  Copyright (c) 2024 Huawei Technologies
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Huawei Technologies - initial API and implementation
 *
 */

package com.huawei.cloud.transfer.obs;

import com.obs.services.ObsClient;
import com.obs.services.model.PutObjectRequest;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.InputStreamDataSource;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;

import static org.eclipse.edc.junit.assertions.AbstractResultAssert.assertThat;
import static org.mockito.Mockito.mock;

abstract class ObsDataSinkTestBase {
    private static final int SIZE_5MB = 1024 * 1024 * 5;
    protected static File testFile;
    public String bucketName;

    @BeforeAll
    static void prepare() throws IOException {
        // create a fairly large file of 64MB
        testFile = Files.createTempFile("", ".bin").toFile();
        byte[] buffer = new byte[SIZE_5MB];
        new Random().nextBytes(buffer);
        try (var fos = new FileOutputStream(testFile, false)) {
            fos.write(buffer);
        }
    }


    @BeforeEach
    void setup(TestInfo testInfo) {
        bucketName = createBucket(testInfo);

    }

    @NotNull
    protected abstract String createBucket(TestInfo testInfo);

    @Test
    void uploadFile() {
        var sink = ObsDataSink.Builder.newInstance()
                .client(getObsClient())
                .bucketName(bucketName)
                .chunkSizeBytes(1024 * 1024 * 10) // 10mb
                .requestId(UUID.randomUUID().toString())
                .executorService(Executors.newFixedThreadPool(20))
                .monitor(mock())
                .build();

        var result = sink.transferParts(List.of(createPart(testFile)));
        assertThat(result).withFailMessage(result::getFailureDetail).isSucceeded();
    }

    @Test
    void uploadFile_alreadyExists() {
        var req = new PutObjectRequest(bucketName, testFile.getName(), testFile);

        getObsClient().putObject(req);
        var sink = ObsDataSink.Builder.newInstance()
                .client(getObsClient())
                .bucketName(bucketName)
                .chunkSizeBytes(1024 * 1024 * 10) // 10mb
                .requestId(UUID.randomUUID().toString())
                .executorService(Executors.newFixedThreadPool(20))
                .monitor(mock())
                .build();

        var result = sink.transferParts(List.of(createPart(testFile)));
        assertThat(result).withFailMessage(result::getFailureDetail).isSucceeded();
    }

    @Test
    void uploadFile_whenBucketNotExist() {
        var bucket = "not-exist-bucket";
        var sink = ObsDataSink.Builder.newInstance()
                .client(getObsClient())
                .bucketName(bucket)
                .chunkSizeBytes(1024 * 1024 * 10) // 10mb
                .requestId(UUID.randomUUID().toString())
                .executorService(Executors.newFixedThreadPool(20))
                .monitor(mock())
                .build();

        var result = sink.transferParts(List.of(createPart(testFile)));
        assertThat(result).isFailed().detail().isEqualTo("Error writing part 1 of the %s object on the %s bucket: The specified bucket does not exist.".formatted(testFile.getName(), bucket));
    }

    @Disabled("Large files are problematic with MinIO for some reason.")
    @ParameterizedTest(name = "File size bytes: {0}")
    @ValueSource(longs = {
            1024L * 1024L * 500, // 500mb
            1024L * 1024L * 1024, // 1gb
            1024L * 1024L * 1024 * 5  // 5gb
    })
    void uploadLargeFile_500mb(long filesize) throws FileNotFoundException {
        var file = createSparseFile(filesize);

        var sink = ObsDataSink.Builder.newInstance()
                .client(getObsClient())
                .bucketName(bucketName)
                .chunkSizeBytes(1024 * 1024 * 10) // 100mb
                .requestId(UUID.randomUUID().toString())
                .executorService(Executors.newFixedThreadPool(20))
                .monitor(mock())
                .build();

        var part = new InputStreamDataSource("large-file", new FileInputStream(file));
        var result = sink.transferParts(List.of(part));
        assertThat(result).withFailMessage(result::getFailureDetail).isSucceeded();
    }

    private DataSource.Part createPart(File file) {
        try {
            return new InputStreamDataSource(file.getName(), new FileInputStream(file));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private File createSparseFile(long filesizeBytes) {
        try {
            var name = Files.createTempFile("obsitest-", ".bin");
            try (var f = new RandomAccessFile(name.toFile(), "rw")) {
                f.setLength(filesizeBytes);
                f.write("foobar".getBytes());
            }
            return name.toFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    protected abstract ObsClient getObsClient();
}
