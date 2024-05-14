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

package com.huawei.cloud.obs;

public interface ObsBucketSchema {
    String TYPE = "OBS";
    String BUCKET_NAME = "bucketName";
    String KEY_PREFIX = "keyPrefix";
    String ACCESS_KEY_ID = "accessKeyId";
    String SECRET_ACCESS_KEY = "secretAccessKey";
    String ENDPOINT = "endpoint";
}
