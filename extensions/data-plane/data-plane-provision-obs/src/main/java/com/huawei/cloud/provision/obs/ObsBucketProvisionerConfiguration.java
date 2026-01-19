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

package com.huawei.cloud.provision.obs;

import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.runtime.metamodel.annotation.Settings;

@Settings
public record ObsBucketProvisionerConfiguration(
        @Setting(
                key = "edc.obs.provision.token.duration",
                description = "Duration for which the temporary token is valid"
        )
        int tokenDuration
) {
}
