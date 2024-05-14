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

package com.huawei.cloud.gaussdb.testfixtures;

import com.huawei.gauss200.jdbc.ds.PGPoolingDataSource;
import com.huawei.gauss200.jdbc.util.PSQLException;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import javax.sql.DataSource;

import static org.eclipse.edc.util.configuration.ConfigurationFunctions.propOrEnv;

public class GaussDbPostgresFunctions {
    private static final String PG_CONNECTION_STRING = "PG_CONNECTION_STRING";

    private static final int TIMEOUT_SEC = 100;

    @NotNull
    private static DataSource createDatasource(String connectionString) {
        var ds = new PGPoolingDataSource();
        try {
            ds.setURL(connectionString);
            ds.setInitialConnections(20);
            ds.setMaxConnections(100);
            ds.setConnectTimeout(TIMEOUT_SEC);
            ds.setSocketTimeout(TIMEOUT_SEC);
            ds.setLoginTimeout(TIMEOUT_SEC);
            ds.setCancelSignalTimeout(TIMEOUT_SEC);
        } catch (PSQLException e) {
            throw new RuntimeException(e);
        }
        return ds;
    }


    public static DataSource createDataSource() {
        var connectionString = propOrEnv(PG_CONNECTION_STRING, null);
        Objects.requireNonNull(connectionString, "GaussDB Postgres connection string not found");
        return createDatasource(connectionString);
    }
}
