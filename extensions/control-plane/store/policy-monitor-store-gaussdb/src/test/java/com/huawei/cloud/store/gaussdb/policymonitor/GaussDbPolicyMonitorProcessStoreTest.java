package com.huawei.cloud.store.gaussdb.policymonitor;

import com.huawei.cloud.gaussdb.testfixtures.GaussDbTestExtension;
import com.huawei.cloud.gaussdb.testfixtures.annotations.GaussDbTest;
import org.eclipse.edc.connector.policy.monitor.spi.PolicyMonitorEntry;
import org.eclipse.edc.connector.policy.monitor.spi.PolicyMonitorEntryStates;
import org.eclipse.edc.connector.policy.monitor.spi.PolicyMonitorStore;
import org.eclipse.edc.connector.policy.monitor.store.sql.SqlPolicyMonitorStore;
import org.eclipse.edc.connector.policy.monitor.store.sql.schema.PostgresPolicyMonitorStatements;
import org.eclipse.edc.junit.assertions.AbstractResultAssert;
import org.eclipse.edc.policy.model.PolicyRegistrationTypes;
import org.eclipse.edc.spi.entity.Entity;
import org.eclipse.edc.spi.entity.MutableEntity;
import org.eclipse.edc.spi.entity.StatefulEntity;
import org.eclipse.edc.spi.result.StoreFailure;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.sql.QueryExecutor;
import org.eclipse.edc.sql.lease.testfixtures.LeaseUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Duration;
import java.util.UUID;

import static com.huawei.cloud.gaussdb.testfixtures.GaussDbTestExtension.DEFAULT_DATASOURCE_NAME;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.connector.policy.monitor.spi.PolicyMonitorEntryStates.COMPLETED;
import static org.eclipse.edc.connector.policy.monitor.spi.PolicyMonitorEntryStates.STARTED;
import static org.eclipse.edc.spi.persistence.StateEntityStore.hasState;
import static org.eclipse.edc.spi.result.StoreFailure.Reason.ALREADY_LEASED;
import static org.eclipse.edc.spi.result.StoreFailure.Reason.NOT_FOUND;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;
import static org.testcontainers.shaded.org.hamcrest.Matchers.hasSize;

@GaussDbTest
@ExtendWith(GaussDbTestExtension.class)
class GaussDbPolicyMonitorProcessStoreTest {

    protected static final String CONNECTOR_NAME = "test-connector";
    private static final PostgresPolicyMonitorStatements SQL_STATEMENTS = new PostgresPolicyMonitorStatements();
    protected final Clock clock = Clock.systemUTC();
    private LeaseUtil leaseUtil;
    private SqlPolicyMonitorStore policyMonitorStore;

    @BeforeEach
    void setUp(GaussDbTestExtension extension, GaussDbTestExtension.SqlHelper helper, QueryExecutor queryExecutor) {
        var clock = Clock.systemUTC();
        var typeManager = new TypeManager();
        typeManager.registerTypes(PolicyRegistrationTypes.TYPES.toArray(Class<?>[]::new));

        policyMonitorStore = new SqlPolicyMonitorStore(extension.getRegistry(), DEFAULT_DATASOURCE_NAME,
                extension.getTransactionContext(), SQL_STATEMENTS, typeManager.getMapper(), clock, queryExecutor, CONNECTOR_NAME);

        leaseUtil = new LeaseUtil(extension.getTransactionContext(), extension::newConnection, SQL_STATEMENTS, clock);

        helper.truncateTable(SQL_STATEMENTS.getPolicyMonitorTable());
        helper.truncateTable(SQL_STATEMENTS.getLeaseTableName());
    }

    @Test
    void nextNotLeased_shouldReturnNotLeasedItems() {
        var state = STARTED;
        var all = range(0, 5)
                .mapToObj(i -> createPolicyMonitorEntry("id-" + i, state))
                .peek(getStore()::save)
                .peek(this::delayByTenMillis)
                .toList();

        var leased = getStore().nextNotLeased(2, hasState(state.code()));

        assertThat(leased).hasSize(2).extracting(PolicyMonitorEntry::getId)
                .isSubsetOf(all.stream().map(Entity::getId).toList())
                .allMatch(id -> isLeasedBy(id, CONNECTOR_NAME));

        assertThat(leased).extracting(MutableEntity::getUpdatedAt).isSorted();
    }

    @Test
    void nextNotLeased_shouldReturnFreeEntities() {
        var state = STARTED;
        var all = range(0, 5)
                .mapToObj(i -> createPolicyMonitorEntry("id-" + i, state))
                .peek(getStore()::save)
                .toList();

        var firstLeased = getStore().nextNotLeased(2, hasState(state.code()));
        var leased = getStore().nextNotLeased(2, hasState(state.code()));

        assertThat(leased.stream().map(Entity::getId)).hasSize(2)
                .isSubsetOf(all.stream().map(Entity::getId).toList())
                .doesNotContainAnyElementsOf(firstLeased.stream().map(Entity::getId).toList());
    }

    @Test
    void nextNotLeased_shouldReturnFreeItemInTheExpectedState() {
        range(0, 5)
                .mapToObj(i -> createPolicyMonitorEntry("id-" + i, STARTED))
                .forEach(getStore()::save);

        var leased = getStore().nextNotLeased(2, hasState(COMPLETED.code()));

        assertThat(leased).isEmpty();
    }

    @Test
    void nextNotLeased_shouldLeaseAgainAfterTimePassed() {
        var entry = createPolicyMonitorEntry(UUID.randomUUID().toString(), STARTED);
        getStore().save(entry);

        leaseEntity(entry.getId(), CONNECTOR_NAME, Duration.ofMillis(100));

        await().atMost(timeout())
                .until(() -> getStore().nextNotLeased(1, hasState(STARTED.code())), hasSize(1));
    }

    @Test
    void nextNotLeased_shouldReturnReleasedEntityByUpdate() {
        var entry = createPolicyMonitorEntry(UUID.randomUUID().toString(), STARTED);
        getStore().save(entry);

        var firstLeased = getStore().nextNotLeased(1, hasState(STARTED.code()));
        assertThat(firstLeased).hasSize(1);

        var secondLeased = getStore().nextNotLeased(1, hasState(STARTED.code()));
        assertThat(secondLeased).isEmpty();

        getStore().save(firstLeased.get(0));

        var thirdLeased = getStore().nextNotLeased(1, hasState(STARTED.code()));
        assertThat(thirdLeased).hasSize(1);
    }

    @Test
    void create_shouldStoreEntity_whenItDoesNotAlreadyExist() {
        var entry = createPolicyMonitorEntry(UUID.randomUUID().toString(), STARTED);
        getStore().save(entry);

        var result = getStore().findById(entry.getId());

        assertThat(result).isNotNull().usingRecursiveComparison().isEqualTo(entry);
        assertThat(result.getCreatedAt()).isGreaterThan(0);
    }

    @Test
    void create_shouldUpdate_whenEntityAlreadyExist() {
        var entry = createPolicyMonitorEntry(UUID.randomUUID().toString(), STARTED);
        getStore().save(entry);

        entry.transitionToCompleted();
        getStore().save(entry);

        var result = getStore().findById(entry.getId());

        assertThat(result).isNotNull();
        assertThat(result.getState()).isEqualTo(COMPLETED.code());
    }

    @Test
    void findByIdAndLease_shouldReturnTheEntityAndLeaseIt() {
        var id = UUID.randomUUID().toString();
        getStore().save(createPolicyMonitorEntry(id, STARTED));

        var result = getStore().findByIdAndLease(id);

        AbstractResultAssert.assertThat(result).isSucceeded();
        assertThat(isLeasedBy(id, CONNECTOR_NAME)).isTrue();
    }

    @Test
    void findByIdAndLease_shouldReturnNotFound_whenEntityDoesNotExist() {
        var result = getStore().findByIdAndLease("unexistent");

        AbstractResultAssert.assertThat(result).isFailed().extracting(StoreFailure::getReason).isEqualTo(NOT_FOUND);
    }

    @Test
    void findByIdAndLease_shouldReturnAlreadyLeased_whenEntityIsAlreadyLeased() {
        var id = UUID.randomUUID().toString();
        getStore().save(createPolicyMonitorEntry(id, STARTED));
        leaseEntity(id, "other owner");

        var result = getStore().findByIdAndLease(id);

        AbstractResultAssert.assertThat(result).isFailed().extracting(StoreFailure::getReason).isEqualTo(ALREADY_LEASED);
    }

    protected void leaseEntity(String negotiationId, String owner) {
        leaseEntity(negotiationId, owner, Duration.ofSeconds(60));
    }

    protected PolicyMonitorStore getStore() {
        return policyMonitorStore;
    }

    protected void leaseEntity(String negotiationId, String owner, Duration duration) {
        leaseUtil.leaseEntity(negotiationId, owner, duration);
    }

    protected boolean isLeasedBy(String negotiationId, String owner) {
        return leaseUtil.isLeased(negotiationId, owner);
    }

    private Duration timeout() {
        return Duration.ofMinutes(1);
    }

    private PolicyMonitorEntry createPolicyMonitorEntry(String id, PolicyMonitorEntryStates state) {
        return PolicyMonitorEntry.Builder.newInstance()
                .id(id)
                .contractId(UUID.randomUUID().toString())
                .state(state.code())
                .build();
    }

    private void delayByTenMillis(StatefulEntity<?> t) {
        try {
            Thread.sleep(10);
        } catch (InterruptedException ignored) {
            // noop
        }
        t.updateStateTimestamp();
    }

    @BeforeAll
    static void createDatabase(GaussDbTestExtension.SqlHelper runner) throws IOException {
        var schema = Files.readString(Paths.get("docs/schema.sql"));
        runner.executeStatement(schema);
    }

    @AfterAll
    static void deleteTable(GaussDbTestExtension.SqlHelper runner) {
        runner.dropTable(SQL_STATEMENTS.getPolicyMonitorTable());
        runner.dropTable(SQL_STATEMENTS.getLeaseTableName());
    }
}