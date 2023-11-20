package com.huawei.cloud.store.gaussdb.assetindex;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.huawei.cloud.gaussdb.testfixtures.GaussDbTestExtension;
import com.huawei.cloud.gaussdb.testfixtures.annotations.GaussDbTest;
import org.eclipse.edc.connector.dataplane.selector.spi.instance.DataPlaneInstance;
import org.eclipse.edc.connector.dataplane.selector.spi.store.DataPlaneInstanceStore;
import org.eclipse.edc.connector.dataplane.selector.spi.testfixtures.TestFunctions;
import org.eclipse.edc.connector.dataplane.selector.store.sql.SqlDataPlaneInstanceStore;
import org.eclipse.edc.connector.dataplane.selector.store.sql.schema.DataPlaneInstanceStatements;
import org.eclipse.edc.connector.dataplane.selector.store.sql.schema.postgres.PostgresDataPlaneInstanceStatements;
import org.eclipse.edc.sql.QueryExecutor;
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

import static com.huawei.cloud.gaussdb.testfixtures.GaussDbTestExtension.DEFAULT_DATASOURCE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.spi.result.StoreFailure.Reason.ALREADY_EXISTS;

@GaussDbTest
@ExtendWith(GaussDbTestExtension.class)
class GaussDbDataPlaneInstanceStoreTest {
    private static final DataPlaneInstanceStatements SQL_STATEMENTS = new PostgresDataPlaneInstanceStatements();
    private DataPlaneInstanceStore dataPlaneInstanceStore;

    @BeforeAll
    static void prepare(GaussDbTestExtension.SqlHelper runner) throws IOException {
        runner.executeStatement(Files.readString(Paths.get("docs/schema.sql")));
    }

    @AfterAll
    static void deleteTable(GaussDbTestExtension.SqlHelper runner) {
        runner.dropTable(SQL_STATEMENTS.getDataPlaneInstanceTable());
    }

    @BeforeEach
    void setup(GaussDbTestExtension.SqlHelper runner, TransactionContext transactionContext, QueryExecutor queryExecutor, DataSourceRegistry reg) {
        dataPlaneInstanceStore = new SqlDataPlaneInstanceStore(reg, DEFAULT_DATASOURCE_NAME, transactionContext, SQL_STATEMENTS, new ObjectMapper(), queryExecutor);

        runner.truncateTable(SQL_STATEMENTS.getDataPlaneInstanceTable());
    }

    @Test
    void save() {
        var inst = TestFunctions.createInstance("test-id");
        getStore().create(inst);
        assertThat(getStore().getAll()).usingRecursiveFieldByFieldElementComparator().containsExactly(inst);
    }

    @Test
    void save_whenExists_shouldNotUpsert() {
        var inst = TestFunctions.createInstance("test-id");
        getStore().create(inst);

        var inst2 = DataPlaneInstance.Builder.newInstance()
                .id("test-id")
                .url("http://somewhere.other:9876/api/v2") //different URL
                .build();

        var result = getStore().create(inst2);

        assertThat(result.failed()).isTrue();
        assertThat(result.getFailure().getReason()).isEqualTo(ALREADY_EXISTS);

        assertThat(getStore().getAll()).hasSize(1).usingRecursiveFieldByFieldElementComparator().containsExactly(inst);
    }

    @Test
    void update_whenExists_shouldUpdate() {
        var inst = TestFunctions.createInstance("test-id");
        getStore().create(inst);


        var inst2 = DataPlaneInstance.Builder.newInstance()
                .id("test-id")
                .url("http://somewhere.other:9876/api/v2") //different URL
                .build();

        var result = getStore().update(inst2);

        assertThat(result.succeeded()).isTrue();

        assertThat(getStore().getAll()).hasSize(1).usingRecursiveFieldByFieldElementComparator().containsExactly(inst2);
    }
    
    @Test
    void save_shouldReturnCustomInstance() {
        var custom = TestFunctions.createCustomInstance("test-id", "name");

        getStore().create(custom);

        var customInstance = getStore().findById(custom.getId());


        assertThat(customInstance)
                .isInstanceOf(DataPlaneInstance.class)
                .usingRecursiveComparison()
                .isEqualTo(custom);
    }

    @Test
    void findById() {
        var inst = TestFunctions.createInstance("test-id");
        getStore().create(inst);

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

        store.create(doc1);
        store.create(doc2);

        var foundItems = store.getAll();

        assertThat(foundItems).isNotNull().hasSize(2);
    }

    protected DataPlaneInstanceStore getStore() {
        return dataPlaneInstanceStore;
    }

}