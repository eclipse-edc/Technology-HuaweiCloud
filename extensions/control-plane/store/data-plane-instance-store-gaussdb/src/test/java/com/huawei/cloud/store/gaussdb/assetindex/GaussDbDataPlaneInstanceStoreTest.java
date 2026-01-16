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

package com.huawei.cloud.store.gaussdb.assetindex;

import com.huawei.cloud.gaussdb.testfixtures.GaussDbTestExtension;
import com.huawei.cloud.gaussdb.testfixtures.annotations.GaussDbTest;
import org.eclipse.edc.connector.dataplane.selector.spi.instance.DataPlaneInstance;
import org.eclipse.edc.connector.dataplane.selector.spi.store.DataPlaneInstanceStore;
import org.eclipse.edc.connector.dataplane.selector.store.sql.SqlDataPlaneInstanceStore;
import org.eclipse.edc.connector.dataplane.selector.store.sql.schema.DataPlaneInstanceStatements;
import org.eclipse.edc.connector.dataplane.selector.store.sql.schema.postgres.PostgresDataPlaneInstanceStatements;
import org.eclipse.edc.json.JacksonTypeManager;
import org.eclipse.edc.sql.QueryExecutor;
import org.eclipse.edc.sql.lease.BaseSqlLeaseStatements;
import org.eclipse.edc.sql.lease.SqlLeaseContextBuilderImpl;
import org.eclipse.edc.sql.lease.spi.LeaseStatements;
import org.eclipse.edc.transaction.datasource.spi.DataSourceRegistry;
import org.eclipse.edc.transaction.spi.TransactionContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Clock;

import static com.huawei.cloud.gaussdb.testfixtures.GaussDbTestExtension.DEFAULT_DATASOURCE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

@GaussDbTest
@ExtendWith(GaussDbTestExtension.class)
class GaussDbDataPlaneInstanceStoreTest {
    protected static final String CONNECTOR_NAME = "test-connector";
    private static final LeaseStatements LEASE_STATEMENTS = new BaseSqlLeaseStatements();
    private static final DataPlaneInstanceStatements SQL_STATEMENTS = new PostgresDataPlaneInstanceStatements(LEASE_STATEMENTS, Clock.systemUTC());
    private DataPlaneInstanceStore dataPlaneInstanceStore;

    @BeforeAll
    static void prepare(GaussDbTestExtension.SqlHelper runner) throws IOException {
        runner.executeStatement(Files.readString(Paths.get("docs/schema.sql")));
    }

    @AfterAll
    static void deleteTable(GaussDbTestExtension.SqlHelper runner) {
        runner.dropTable("edc_data_plane_instance");
    }

    @BeforeEach
    void setup(GaussDbTestExtension extension, GaussDbTestExtension.SqlHelper runner, TransactionContext transactionContext, QueryExecutor queryExecutor, DataSourceRegistry reg) {
        var typeManager = new JacksonTypeManager();
        typeManager.registerTypes(DataPlaneInstance.class);
        var leaseContextBuilder = SqlLeaseContextBuilderImpl.with(extension.getTransactionContext(), CONNECTOR_NAME, SQL_STATEMENTS.getDataPlaneInstanceTable(), LEASE_STATEMENTS, Clock.systemUTC(), queryExecutor);
        dataPlaneInstanceStore = new SqlDataPlaneInstanceStore(extension.getRegistry(), DEFAULT_DATASOURCE_NAME,
                extension.getTransactionContext(), SQL_STATEMENTS, leaseContextBuilder, typeManager.getMapper(), queryExecutor);
        runner.truncateTable("edc_data_plane_instance");
    }

    @Test
    void save() {
        var inst = TestFunctions.createInstance("test-id");
        getStore().save(inst);
        assertThat(getStore().getAll()).usingRecursiveFieldByFieldElementComparator().containsExactly(inst);
    }

    @Test
    void update_whenExists_shouldUpdate() {
        var inst = TestFunctions.createInstance("test-id");
        getStore().save(inst);


        var inst2 = DataPlaneInstance.Builder.newInstance()
                .id("test-id")
                .url("http://somewhere.other:9876/api/v2") //different URL
                .build();

        getStore().save(inst2);

        assertThat(getStore().getAll()).hasSize(1).usingRecursiveFieldByFieldElementComparator().containsExactly(inst2);
    }

    @Test
    void save_shouldReturnCustomInstance() {
        var custom = TestFunctions.createCustomInstance("test-id", "name");

        getStore().save(custom);

        var customInstance = getStore().findById(custom.getId());


        assertThat(customInstance)
                .isInstanceOf(DataPlaneInstance.class)
                .usingRecursiveComparison()
                .isEqualTo(custom);
    }

    @Test
    void findById() {
        var inst = TestFunctions.createInstance("test-id");
        getStore().save(inst);

        assertThat(getStore().findById("test-id")).usingRecursiveComparison().isEqualTo(inst);
    }

    @Test
    void findById_notExists() {
        assertThat(getStore().findById("not-exist")).isNull();
    }

    @Test
    void getAll() {
        var doc1 = TestFunctions.createCustomInstance("test-id", "name");
        var doc2 = TestFunctions.createCustomInstance("test-id-2", "name");

        var store = getStore();

        store.save(doc1);
        store.save(doc2);

        var foundItems = store.getAll();

        assertThat(foundItems).isNotNull().hasSize(2);
    }

    protected DataPlaneInstanceStore getStore() {
        return dataPlaneInstanceStore;
    }

    public class TestFunctions {
        TestFunctions() {
        }

        public static DataPlaneInstance createInstance(String id) {
            return org.eclipse.edc.connector.dataplane.selector.spi.instance.DataPlaneInstance.Builder.newInstance().id(id).url("http://somewhere.com:1234/api/v1").build();
        }

        public static DataPlaneInstance createCustomInstance(String id, String name) {
            return org.eclipse.edc.connector.dataplane.selector.spi.instance.DataPlaneInstance.Builder.newInstance().id(id).url("http://somewhere.com:1234/api/v1").property("name", "name").build();
        }
    }

}
