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

package com.huawei.cloud.store.gaussdb.transferprocess;

import org.eclipse.edc.connector.controlplane.store.sql.transferprocess.store.schema.postgres.PostgresDialectStatements;
import org.eclipse.edc.spi.query.QuerySpec;
import org.eclipse.edc.sql.lease.spi.LeaseStatements;
import org.eclipse.edc.sql.translation.SqlQueryStatement;

import java.time.Clock;

public class GaussDbStatements extends PostgresDialectStatements {

    public GaussDbStatements(LeaseStatements leaseStatements, Clock clock) {
        super(leaseStatements, clock);
    }

    @Override
    public SqlQueryStatement createQuery(QuerySpec querySpec) {
        return super.createQuery(querySpec);
    }
}
