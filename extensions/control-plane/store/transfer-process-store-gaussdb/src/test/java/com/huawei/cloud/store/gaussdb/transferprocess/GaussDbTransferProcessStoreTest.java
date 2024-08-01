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

import com.huawei.cloud.gaussdb.testfixtures.GaussDbTestExtension;
import com.huawei.cloud.gaussdb.testfixtures.annotations.GaussDbTest;
import org.eclipse.edc.connector.controlplane.store.sql.transferprocess.store.SqlTransferProcessStore;
import org.eclipse.edc.connector.controlplane.store.sql.transferprocess.store.schema.postgres.PostgresDialectStatements;
import org.eclipse.edc.connector.controlplane.transfer.spi.store.TransferProcessStore;
import org.eclipse.edc.connector.controlplane.transfer.spi.testfixtures.store.TestFunctions;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.DeprovisionedResource;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.ProvisionedResourceSet;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.ResourceManifest;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcess;
import org.eclipse.edc.json.JacksonTypeManager;
import org.eclipse.edc.junit.assertions.AbstractResultAssert;
import org.eclipse.edc.policy.model.PolicyRegistrationTypes;
import org.eclipse.edc.spi.query.Criterion;
import org.eclipse.edc.spi.query.QuerySpec;
import org.eclipse.edc.spi.query.SortOrder;
import org.eclipse.edc.spi.result.StoreFailure;
import org.eclipse.edc.spi.types.domain.callback.CallbackAddress;
import org.eclipse.edc.sql.QueryExecutor;
import org.eclipse.edc.sql.lease.testfixtures.LeaseUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.huawei.cloud.gaussdb.testfixtures.GaussDbTestExtension.DEFAULT_DATASOURCE_NAME;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.eclipse.edc.connector.controlplane.transfer.spi.testfixtures.store.TestFunctions.createTransferProcess;
import static org.eclipse.edc.connector.controlplane.transfer.spi.testfixtures.store.TestFunctions.createTransferProcessBuilder;
import static org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcessStates.INITIAL;
import static org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcessStates.PROVISIONING;
import static org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcessStates.STARTED;
import static org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcessStates.TERMINATED;
import static org.eclipse.edc.spi.persistence.StateEntityStore.hasState;
import static org.eclipse.edc.spi.result.StoreFailure.Reason.ALREADY_LEASED;
import static org.eclipse.edc.spi.result.StoreFailure.Reason.NOT_FOUND;
import static org.testcontainers.shaded.org.hamcrest.Matchers.hasSize;

@GaussDbTest
@ExtendWith(GaussDbTestExtension.class)
class GaussDbTransferProcessStoreTest {

    protected static final String CONNECTOR_NAME = "test-connector";
    private static final PostgresDialectStatements SQL_STATEMENTS = new GaussDbStatements();
    protected final Clock clock = Clock.systemUTC();
    private LeaseUtil leaseUtil;
    private TransferProcessStore transferProcessStore;

    @BeforeEach
    void setUp(GaussDbTestExtension extension, GaussDbTestExtension.SqlHelper helper, QueryExecutor queryExecutor) {
        var clock = Clock.systemUTC();
        var typeManager = new JacksonTypeManager();
        typeManager.registerTypes(TestFunctions.TestResourceDef.class, TestFunctions.TestProvisionedResource.class);
        typeManager.registerTypes(PolicyRegistrationTypes.TYPES.toArray(Class<?>[]::new));

        transferProcessStore = new SqlTransferProcessStore(extension.getRegistry(), DEFAULT_DATASOURCE_NAME,
                extension.getTransactionContext(), typeManager.getMapper(), SQL_STATEMENTS, CONNECTOR_NAME, clock, queryExecutor);

        leaseUtil = new LeaseUtil(extension.getTransactionContext(), extension::newConnection, SQL_STATEMENTS, clock);

        helper.truncateTable(SQL_STATEMENTS.getTransferProcessTableName());
        helper.truncateTable(SQL_STATEMENTS.getLeaseTableName());
    }

    @Test
    void create_shouldCreateTheEntity() {
        var transferProcess = createTransferProcessBuilder("test-id")
                .correlationId("data-request-id")
                .privateProperties(Map.of("key", "value")).build();
        getTransferProcessStore().save(transferProcess);

        var retrieved = getTransferProcessStore().findById("test-id");

        assertThat(retrieved).isNotNull().usingRecursiveComparison().isEqualTo(transferProcess);
        assertThat(retrieved.getCreatedAt()).isNotEqualTo(0L);
    }

    @Test
    void create_verifyCallbacks() {

        var callbacks = List.of(CallbackAddress.Builder.newInstance().uri("test").events(Set.of("event")).build());

        var t = createTransferProcessBuilder("test-id").privateProperties(Map.of("key", "value")).callbackAddresses(callbacks).build();
        getTransferProcessStore().save(t);

        var all = getTransferProcessStore().findAll(QuerySpec.none()).collect(Collectors.toList());
        assertThat(all).containsExactly(t);
        assertThat(all.get(0)).usingRecursiveComparison().isEqualTo(t);
        assertThat(all.get(0).getCallbackAddresses()).hasSize(1).usingRecursiveFieldByFieldElementComparator().containsAll(callbacks);
    }

    @Test
    void create_verifyTransferType() {
        var t = TestFunctions.createTransferProcessBuilder("test-id").transferType("transferType").build();
        getTransferProcessStore().save(t);

        var all = getTransferProcessStore().findAll(QuerySpec.none()).collect(Collectors.toList());
        assertThat(all).containsExactly(t);
        assertThat(all.get(0)).usingRecursiveComparison().isEqualTo(t);
        assertThat(all.get(0).getTransferType()).isEqualTo("transferType");
    }

    @Test
    void create_verifyDataPlaneId() {
        var t = TestFunctions.createTransferProcessBuilder("test-id").dataPlaneId("dataPlaneId").build();
        getTransferProcessStore().save(t);

        var all = getTransferProcessStore().findAll(QuerySpec.none()).collect(Collectors.toList());
        assertThat(all).containsExactly(t);
        assertThat(all.get(0)).usingRecursiveComparison().isEqualTo(t);
        assertThat(all.get(0).getDataPlaneId()).isEqualTo("dataPlaneId");
    }

    @Test
    void create_withSameIdExists_shouldReplace() {
        var t = createTransferProcess("id1", INITIAL);
        getTransferProcessStore().save(t);

        var t2 = createTransferProcess("id1", PROVISIONING);
        getTransferProcessStore().save(t2);

        assertThat(getTransferProcessStore().findAll(QuerySpec.none())).hasSize(1).containsExactly(t2);
    }

    @Test
    void nextNotLeased_shouldReturnNotLeasedItems() {
        var state = STARTED;
        var all = range(0, 10)
                .mapToObj(i -> createTransferProcess("id" + i, state))
                .peek(getTransferProcessStore()::save)
                .toList();

        assertThat(getTransferProcessStore().nextNotLeased(5, hasState(state.code())))
                .hasSize(5)
                .extracting(TransferProcess::getId)
                .isSubsetOf(all.stream().map(TransferProcess::getId).collect(Collectors.toList()))
                .allMatch(id -> isLeasedBy(id, CONNECTOR_NAME));
    }

    @Test
    void nextNotLeased_shouldOnlyReturnFreeItems() {
        var state = STARTED;
        var all = range(0, 10)
                .mapToObj(i -> createTransferProcess("id" + i, state))
                .peek(getTransferProcessStore()::save)
                .collect(Collectors.toList());

        // lease a few
        var leasedTp = all.stream().skip(5).peek(tp -> leaseEntity(tp.getId(), CONNECTOR_NAME)).toList();

        // should not contain leased TPs
        assertThat(getTransferProcessStore().nextNotLeased(10, hasState(state.code())))
                .hasSize(5)
                .isSubsetOf(all)
                .doesNotContainAnyElementsOf(leasedTp);
    }

    @Test
    void nextNotLeased_noFreeItem_shouldReturnEmpty() {
        var state = STARTED;
        range(0, 3)
                .mapToObj(i -> createTransferProcess("id" + i, state))
                .forEach(getTransferProcessStore()::save);

        // first time works
        assertThat(getTransferProcessStore().nextNotLeased(10, hasState(state.code()))).hasSize(3);
        // second time returns empty list
        assertThat(getTransferProcessStore().nextNotLeased(10, hasState(state.code()))).isEmpty();
    }

    @Test
    void nextNotLeased_noneInDesiredState() {
        range(0, 3)
                .mapToObj(i -> createTransferProcess("id" + i, STARTED))
                .forEach(getTransferProcessStore()::save);

        var nextNotLeased = getTransferProcessStore().nextNotLeased(10, hasState(TERMINATED.code()));

        assertThat(nextNotLeased).isEmpty();
    }

    @Test
    void nextNotLeased_batchSizeLimits() {
        var state = STARTED;
        range(0, 10)
                .mapToObj(i -> createTransferProcess("id" + i, state))
                .forEach(getTransferProcessStore()::save);

        // first time works
        var result = getTransferProcessStore().nextNotLeased(3, hasState(state.code()));
        assertThat(result).hasSize(3);
    }

    @Test
    void nextNotLeased_verifyTemporalOrdering() {
        var state = STARTED;
        range(0, 10)
                .mapToObj(i -> createTransferProcess(String.valueOf(i), state))
                .peek(this::delayByTenMillis)
                .forEach(getTransferProcessStore()::save);

        assertThat(getTransferProcessStore().nextNotLeased(20, hasState(state.code())))
                .extracting(TransferProcess::getId)
                .map(Integer::parseInt)
                .isSortedAccordingTo(Integer::compareTo);
    }

    @Test
    void nextNotLeased_verifyMostRecentlyUpdatedIsLast() throws InterruptedException {
        var all = range(0, 10)
                .mapToObj(i -> createTransferProcess("id" + i, STARTED))
                .peek(getTransferProcessStore()::save)
                .toList();

        Thread.sleep(100);

        var fourth = all.get(3);
        fourth.updateStateTimestamp();
        getTransferProcessStore().save(fourth);

        var next = getTransferProcessStore().nextNotLeased(20, hasState(STARTED.code()));
        assertThat(next.indexOf(fourth)).isEqualTo(9);
    }

    @Test
    @DisplayName("Verifies that calling nextNotLeased locks the TP for any subsequent calls")
    void nextNotLeased_locksEntity() {
        var t = createTransferProcess("id1", INITIAL);
        getTransferProcessStore().save(t);

        getTransferProcessStore().nextNotLeased(100, hasState(INITIAL.code()));

        assertThat(isLeasedBy(t.getId(), CONNECTOR_NAME)).isTrue();
    }

    @Test
    void nextNotLeased_expiredLease() {
        var t = createTransferProcess("id1", INITIAL);
        getTransferProcessStore().save(t);

        leaseEntity(t.getId(), CONNECTOR_NAME, Duration.ofMillis(100));

        Awaitility.await().atLeast(Duration.ofMillis(1000))
                .pollInterval(Duration.ofMillis(1000))
                .atMost(Duration.ofMillis(10000))
                .until(() -> getTransferProcessStore().nextNotLeased(10, hasState(INITIAL.code())), hasSize(1));
    }

    @Test
    void nextNotLeased_shouldLeaseEntityUntilUpdate() {
        var initialTransferProcess = TestFunctions.initialTransferProcess();
        getTransferProcessStore().save(initialTransferProcess);

        var firstQueryResult = getTransferProcessStore().nextNotLeased(1, hasState(INITIAL.code()));
        assertThat(firstQueryResult).hasSize(1);

        var secondQueryResult = getTransferProcessStore().nextNotLeased(1, hasState(INITIAL.code()));
        assertThat(secondQueryResult).hasSize(0);

        var retrieved = firstQueryResult.get(0);
        getTransferProcessStore().save(retrieved);

        var thirdQueryResult = getTransferProcessStore().nextNotLeased(1, hasState(INITIAL.code()));
        assertThat(thirdQueryResult).hasSize(1);
    }

    @Test
    void nextNotLeased_avoidsStarvation() throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            var process = createTransferProcess("test-process-" + i);
            getTransferProcessStore().save(process);
        }

        var list1 = getTransferProcessStore().nextNotLeased(5, hasState(INITIAL.code()));
        Thread.sleep(50); //simulate a short delay to generate different timestamps
        list1.forEach(tp -> {
            tp.updateStateTimestamp();
            getTransferProcessStore().save(tp);
        });
        var list2 = getTransferProcessStore().nextNotLeased(5, hasState(INITIAL.code()));
        assertThat(list1).isNotEqualTo(list2).doesNotContainAnyElementsOf(list2);
    }


    @Test
    void findById_shouldFindEntityById() {
        var t = createTransferProcess("id1");
        getTransferProcessStore().save(t);

        var result = getTransferProcessStore().findById("id1");

        assertThat(result).usingRecursiveComparison().isEqualTo(t);
    }

    @Test
    void findById_notExist() {
        var result = getTransferProcessStore().findById("not-exist");

        assertThat(result).isNull();
    }

    @Test
    void findForCorrelationId_shouldFindEntityByCorrelationId() {
        var transferProcess = createTransferProcessBuilder("id1").correlationId("correlationId").build();
        getTransferProcessStore().save(transferProcess);

        var res = getTransferProcessStore().findForCorrelationId("correlationId");

        assertThat(res).usingRecursiveComparison().isEqualTo(transferProcess);
    }

    @Test
    void findForCorrelationId_notExist() {
        assertThat(getTransferProcessStore().findForCorrelationId("not-exist")).isNull();
    }

    @Test
    void update_exists_shouldUpdate() {
        var t1 = createTransferProcess("id1", STARTED);
        getTransferProcessStore().save(t1);

        t1.transitionCompleted(); //modify
        getTransferProcessStore().save(t1);

        assertThat(getTransferProcessStore().findAll(QuerySpec.none()))
                .hasSize(1)
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactly(t1);
    }

    @Test
    void update_notExist_shouldCreate() {
        var t1 = createTransferProcess("id1", STARTED);

        t1.transitionCompleted(); //modify
        getTransferProcessStore().save(t1);

        var result = getTransferProcessStore().findAll(QuerySpec.none()).collect(Collectors.toList());
        assertThat(result)
                .hasSize(1)
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactly(t1);
    }

    @Test
    @DisplayName("Verify that the lease on a TP is cleared by an update")
    void update_shouldBreakLease() {
        var t1 = createTransferProcess("id1");
        getTransferProcessStore().save(t1);
        // acquire lease
        leaseEntity(t1.getId(), CONNECTOR_NAME);

        t1.transitionProvisioning(ResourceManifest.Builder.newInstance().build()); //modify
        getTransferProcessStore().save(t1);

        // lease should be broken
        var notLeased = getTransferProcessStore().nextNotLeased(10, hasState(PROVISIONING.code()));

        assertThat(notLeased).usingRecursiveFieldByFieldElementComparator().containsExactly(t1);
    }

    @Test
    void update_leasedByOther_shouldThrowException() {
        var tpId = "id1";
        var t1 = createTransferProcess(tpId);
        getTransferProcessStore().save(t1);
        leaseEntity(tpId, "someone");

        t1.transitionProvisioning(ResourceManifest.Builder.newInstance().build()); //modify

        // leased by someone else -> throw exception
        assertThatThrownBy(() -> getTransferProcessStore().save(t1)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void update_shouldReplaceDataRequest_whenItGetsTheIdUpdated() {
        var builder = TestFunctions.createTransferProcessBuilder("id1").state(STARTED.code());
        getTransferProcessStore().save(builder.build());
        var newTransferProcess = builder.correlationId("new-dr-id")
                .assetId("new-asset")
                .contractId("new-contract")
                .protocol("test-protocol").build();
        getTransferProcessStore().save(newTransferProcess);

        var result = getTransferProcessStore().findAll(QuerySpec.none());

        assertThat(result)
                .hasSize(1)
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactly(newTransferProcess);
    }

    @Test
    void delete_shouldDeleteTheEntityById() {
        var t1 = createTransferProcess("id1");
        getTransferProcessStore().save(t1);

        getTransferProcessStore().delete("id1");
        assertThat(getTransferProcessStore().findAll(QuerySpec.none())).isEmpty();
    }

    @Test
    void delete_isLeasedBySelf_shouldThrowException() {
        var t1 = createTransferProcess("id1");
        getTransferProcessStore().save(t1);
        leaseEntity(t1.getId(), CONNECTOR_NAME);


        assertThatThrownBy(() -> getTransferProcessStore().delete("id1")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void delete_isLeasedByOther_shouldThrowException() {
        var t1 = createTransferProcess("id1");
        getTransferProcessStore().save(t1);

        leaseEntity(t1.getId(), "someone-else");

        assertThatThrownBy(() -> getTransferProcessStore().delete("id1")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void delete_notExist() {
        getTransferProcessStore().delete("not-exist");
        //no exception should be raised
    }

    @Test
    void findAll_noQuerySpec() {
        var all = range(0, 10)
                .mapToObj(i -> createTransferProcess("id" + i))
                .peek(getTransferProcessStore()::save)
                .collect(Collectors.toList());

        assertThat(getTransferProcessStore().findAll(QuerySpec.none())).containsExactlyInAnyOrderElementsOf(all);
    }

    @Test
    void findAll_verifyFiltering() {
        range(0, 10).forEach(i -> getTransferProcessStore().save(createTransferProcess("test-neg-" + i)));
        var querySpec = QuerySpec.Builder.newInstance().filter(Criterion.criterion("id", "=", "test-neg-3")).build();

        var result = getTransferProcessStore().findAll(querySpec);

        assertThat(result).extracting(TransferProcess::getId).containsOnly("test-neg-3");
    }

    @Test
    void findAll_shouldThrowException_whenInvalidOperator() {
        range(0, 10).forEach(i -> getTransferProcessStore().save(createTransferProcess("test-neg-" + i)));
        var querySpec = QuerySpec.Builder.newInstance().filter(Criterion.criterion("id", "foobar", "other")).build();

        assertThatThrownBy(() -> getTransferProcessStore().findAll(querySpec).toList()).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void findAll_queryByState() {
        var tp = createTransferProcessBuilder("testprocess1").state(800).build();
        getTransferProcessStore().save(tp);

        var query = QuerySpec.Builder.newInstance()
                .filter(List.of(new Criterion("state", "=", 800)))
                .build();

        var result = getTransferProcessStore().findAll(query).toList();
        assertThat(result).hasSize(1).usingRecursiveFieldByFieldElementComparator().containsExactly(tp);
    }

    @Test
    void findAll_queryByTransferType() {
        range(0, 10).forEach(i -> getTransferProcessStore().save(TestFunctions.createTransferProcessBuilder("test-tp-" + i)
                .transferType("type" + i)
                .build()));
        var querySpec = QuerySpec.Builder.newInstance().filter(Criterion.criterion("transferType", "=", "type4")).build();

        var result = getTransferProcessStore().findAll(querySpec);

        assertThat(result).extracting(TransferProcess::getTransferType).containsOnly("type4");
    }

    @Test
    void findAll_verifySorting() {
        range(0, 10).forEach(i -> getTransferProcessStore().save(createTransferProcess("test-neg-" + i)));

        assertThat(getTransferProcessStore().findAll(QuerySpec.Builder.newInstance().sortField("id").sortOrder(SortOrder.ASC).build())).hasSize(10).isSortedAccordingTo(Comparator.comparing(TransferProcess::getId));
        assertThat(getTransferProcessStore().findAll(QuerySpec.Builder.newInstance().sortField("id").sortOrder(SortOrder.DESC).build())).hasSize(10).isSortedAccordingTo((c1, c2) -> c2.getId().compareTo(c1.getId()));
    }

    @Test
    void findAll_verifyPaging() {
        range(0, 10)
                .mapToObj(i -> createTransferProcess(String.valueOf(i)))
                .forEach(getTransferProcessStore()::save);

        var qs = QuerySpec.Builder.newInstance().limit(5).offset(3).build();
        assertThat(getTransferProcessStore().findAll(qs)).hasSize(5)
                .extracting(TransferProcess::getId)
                .map(Integer::parseInt)
                .allMatch(id -> id >= 3 && id < 8);
    }

    @Test
    void findAll_verifyPaging_pageSizeLargerThanCollection() {

        range(0, 10)
                .mapToObj(i -> createTransferProcess(String.valueOf(i)))
                .forEach(getTransferProcessStore()::save);

        var qs = QuerySpec.Builder.newInstance().limit(20).offset(3).build();
        assertThat(getTransferProcessStore().findAll(qs))
                .hasSize(7)
                .extracting(TransferProcess::getId)
                .map(Integer::parseInt)
                .allMatch(id -> id >= 3 && id < 10);
    }

    @Test
    void findAll_verifyPaging_pageSizeOutsideCollection() {

        range(0, 10)
                .mapToObj(i -> createTransferProcess(String.valueOf(i)))
                .forEach(getTransferProcessStore()::save);

        var qs = QuerySpec.Builder.newInstance().limit(10).offset(12).build();
        assertThat(getTransferProcessStore().findAll(qs)).isEmpty();

    }

    @Test
    void findAll_queryByDataAddressProperty() {
        var da = TestFunctions.createDataAddressBuilder("test-type")
                .property("key", "value")
                .build();
        var tp = createTransferProcessBuilder("testprocess1")
                .contentDataAddress(da)
                .build();
        getTransferProcessStore().save(tp);
        getTransferProcessStore().save(createTransferProcess("testprocess2"));

        var query = QuerySpec.Builder.newInstance()
                .filter(List.of(new Criterion("contentDataAddress.properties.key", "=", "value")))
                .build();

        assertThat(getTransferProcessStore().findAll(query))
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactly(tp);
    }

    @Test
    void findAll_queryByDataAddress_propNotExist() {
        var da = TestFunctions.createDataAddressBuilder("test-type")
                .property("key", "value")
                .build();
        var tp = createTransferProcessBuilder("testprocess1")
                .contentDataAddress(da)
                .build();
        getTransferProcessStore().save(tp);
        getTransferProcessStore().save(createTransferProcess("testprocess2"));

        var query = QuerySpec.Builder.newInstance()
                .filter(List.of(new Criterion("contentDataAddress.properties.notexist", "=", "value")))
                .build();

        assertThat(getTransferProcessStore().findAll(query)).isEmpty();
    }

    @Test
    void findAll_queryByDataAddress_invalidKey_valueNotExist() {
        var da = TestFunctions.createDataAddressBuilder("test-type")
                .property("key", "value")
                .build();
        var tp = createTransferProcessBuilder("testprocess1")
                .contentDataAddress(da)
                .build();
        getTransferProcessStore().save(tp);
        getTransferProcessStore().save(createTransferProcess("testprocess2"));

        var query = QuerySpec.Builder.newInstance()
                .filter(List.of(new Criterion("contentDataAddress.properties.key", "=", "notexist")))
                .build();

        assertThat(getTransferProcessStore().findAll(query)).isEmpty();
    }

    @Test
    void findAll_queryByCorrelationId() {
        var tp = TestFunctions.createTransferProcessBuilder("testprocess1")
                .correlationId("counterPartyId")
                .build();
        getTransferProcessStore().save(tp);
        getTransferProcessStore().save(TestFunctions.createTransferProcess("testprocess2"));

        var query = QuerySpec.Builder.newInstance()
                .filter(List.of(new Criterion("correlationId", "=", "counterPartyId")))
                .build();

        var result = getTransferProcessStore().findAll(query);

        assertThat(result).usingRecursiveFieldByFieldElementComparatorIgnoringFields("deprovisionedResources").containsOnly(tp);
    }

    @Test
    void findAll_queryByDataRequestProperty_protocol() {
        var tp = createTransferProcessBuilder("testprocess1")
                .protocol("test-protocol")
                .build();
        getTransferProcessStore().save(tp);
        getTransferProcessStore().save(createTransferProcess("testprocess2"));

        var query = QuerySpec.Builder.newInstance()
                .filter(List.of(new Criterion("protocol", "like", "test-protocol")))
                .build();

        var result = getTransferProcessStore().findAll(query);

        assertThat(result).usingRecursiveFieldByFieldElementComparatorIgnoringFields("deprovisionedResources").containsOnly(tp);
    }

    @Test
    void findAll_queryByDataRequest_valueNotExist() {
        var tp = createTransferProcessBuilder("testprocess1")
                .build();
        getTransferProcessStore().save(tp);
        getTransferProcessStore().save(createTransferProcess("testprocess2"));

        var query = QuerySpec.Builder.newInstance()
                .filter(List.of(new Criterion("dataRequest.id", "=", "notexist")))
                .build();

        assertThat(getTransferProcessStore().findAll(query)).isEmpty();
    }

    @Test
    void findAll_queryByResourceManifestProperty() {
        var rm = ResourceManifest.Builder.newInstance()
                .definitions(List.of(TestFunctions.TestResourceDef.Builder.newInstance().id("rd-id").transferProcessId("testprocess1").build())).build();
        var tp = createTransferProcessBuilder("testprocess1")
                .resourceManifest(rm)
                .build();
        getTransferProcessStore().save(tp);
        getTransferProcessStore().save(createTransferProcess("testprocess2"));

        var query = QuerySpec.Builder.newInstance()
                .filter(List.of(new Criterion("resourceManifest.definitions.id", "=", "rd-id")))
                .build();

        var result = getTransferProcessStore().findAll(query);
        assertThat(result).usingRecursiveFieldByFieldElementComparatorIgnoringFields("deprovisionedResources").containsOnly(tp);
    }

    @Test
    void findAll_queryByResourceManifest_valueNotExist() {
        var rm = ResourceManifest.Builder.newInstance()
                .definitions(List.of(TestFunctions.TestResourceDef.Builder.newInstance().id("rd-id").transferProcessId("testprocess1").build())).build();
        var tp = createTransferProcessBuilder("testprocess1")
                .resourceManifest(rm)
                .build();
        getTransferProcessStore().save(tp);
        getTransferProcessStore().save(createTransferProcess("testprocess2"));

        // throws exception when an explicit mapping exists
        var query = QuerySpec.Builder.newInstance()
                .filter(List.of(new Criterion("resourceManifest.definitions.id", "=", "someval")))
                .build();
        assertThat(getTransferProcessStore().findAll(query)).isEmpty();
    }

    @Test
    void findAll_queryByProvisionedResourceSetProperty() {
        var resource = TestFunctions.TestProvisionedResource.Builder.newInstance()
                .resourceDefinitionId("rd-id")
                .transferProcessId("testprocess1")
                .id("pr-id")
                .build();
        var prs = ProvisionedResourceSet.Builder.newInstance()
                .resources(List.of(resource))
                .build();
        var tp = createTransferProcessBuilder("testprocess1")
                .provisionedResourceSet(prs)
                .build();
        getTransferProcessStore().save(tp);
        getTransferProcessStore().save(createTransferProcess("testprocess2"));

        var query = QuerySpec.Builder.newInstance()
                .filter(List.of(new Criterion("provisionedResourceSet.resources.transferProcessId", "=", "testprocess1")))
                .build();

        var result = getTransferProcessStore().findAll(query);
        assertThat(result).usingRecursiveFieldByFieldElementComparatorIgnoringFields("deprovisionedResources").containsOnly(tp);
    }

    @Test
    void findAll_queryByProvisionedResourceSet_valueNotExist() {
        var resource = TestFunctions.TestProvisionedResource.Builder.newInstance()
                .resourceDefinitionId("rd-id")
                .transferProcessId("testprocess1")
                .id("pr-id")
                .build();
        var prs = ProvisionedResourceSet.Builder.newInstance()
                .resources(List.of(resource))
                .build();
        var tp = createTransferProcessBuilder("testprocess1")
                .provisionedResourceSet(prs)
                .build();
        getTransferProcessStore().save(tp);
        getTransferProcessStore().save(createTransferProcess("testprocess2"));


        // returns empty when the invalid value is embedded in JSON
        var query = QuerySpec.Builder.newInstance()
                .filter(List.of(new Criterion("provisionedResourceSet.resources.id", "=", "someval")))
                .build();

        assertThat(getTransferProcessStore().findAll(query)).isEmpty();
    }

    @Test
    void findAll_queryByDeprovisionedResourcesProperty() {
        var dp1 = DeprovisionedResource.Builder.newInstance()
                .provisionedResourceId("test-rid1")
                .inProcess(true)
                .build();
        var dp2 = DeprovisionedResource.Builder.newInstance()
                .provisionedResourceId("test-rid2")
                .inProcess(false)
                .build();
        var dp3 = DeprovisionedResource.Builder.newInstance()
                .provisionedResourceId("test-rid3")
                .inProcess(false)
                .build();

        var process1 = createTransferProcessBuilder("test-pid1")
                .deprovisionedResources(List.of(dp1, dp2))
                .build();
        var process2 = createTransferProcessBuilder("test-pid2")
                .deprovisionedResources(List.of(dp3))
                .build();

        getTransferProcessStore().save(process1);
        getTransferProcessStore().save(process2);

        var query = QuerySpec.Builder.newInstance()
                .filter(Criterion.criterion("deprovisionedResources.inProcess", "=", true))
                .build();

        var result = getTransferProcessStore().findAll(query);

        assertThat(result).hasSize(1)
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactly(process1);
    }

    @Test
    void findAll_queryByDeprovisionedResourcesProperty_multipleCriteria() {
        var dp1 = DeprovisionedResource.Builder.newInstance()
                .provisionedResourceId("test-rid1")
                .inProcess(true)
                .build();
        var dp2 = DeprovisionedResource.Builder.newInstance()
                .provisionedResourceId("test-rid2")
                .inProcess(false)
                .build();
        var dp3 = DeprovisionedResource.Builder.newInstance()
                .provisionedResourceId("test-rid3")
                .inProcess(false)
                .build();

        var process1 = createTransferProcessBuilder("test-pid1")
                .deprovisionedResources(List.of(dp1, dp2))
                .build();
        var process2 = createTransferProcessBuilder("test-pid2")
                .deprovisionedResources(List.of(dp3))
                .build();

        getTransferProcessStore().save(process1);
        getTransferProcessStore().save(process2);

        var query = QuerySpec.Builder.newInstance()
                .filter(List.of(
                        new Criterion("deprovisionedResources.inProcess", "=", false),
                        new Criterion("id", "=", "test-pid1")
                ))
                .build();

        var result = getTransferProcessStore().findAll(query).collect(Collectors.toList());

        assertThat(result).hasSize(1)
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactly(process1);
    }

    @Test
    void findAll_queryByDeprovisionedResourcesProperty_multipleResults() {
        var dp1 = DeprovisionedResource.Builder.newInstance()
                .provisionedResourceId("test-rid1")
                .inProcess(true)
                .build();
        var dp2 = DeprovisionedResource.Builder.newInstance()
                .provisionedResourceId("test-rid2")
                .inProcess(false)
                .build();
        var dp3 = DeprovisionedResource.Builder.newInstance()
                .provisionedResourceId("test-rid3")
                .inProcess(false)
                .build();

        var process1 = createTransferProcessBuilder("test-pid1")
                .deprovisionedResources(List.of(dp1, dp2))
                .build();
        var process2 = createTransferProcessBuilder("test-pid2")
                .deprovisionedResources(List.of(dp3))
                .build();

        getTransferProcessStore().save(process1);
        getTransferProcessStore().save(process2);

        var query = QuerySpec.Builder.newInstance()
                .filter(Criterion.criterion("deprovisionedResources.inProcess", "=", false))
                .build();

        var result = getTransferProcessStore().findAll(query).collect(Collectors.toList());

        assertThat(result).hasSize(2)
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactlyInAnyOrder(process1, process2);
    }

    @Test
    void findAll_queryByDeprovisionedResources_propNotExist() {
        var dp1 = DeprovisionedResource.Builder.newInstance()
                .provisionedResourceId("test-rid1")
                .inProcess(true)
                .build();
        var dp2 = DeprovisionedResource.Builder.newInstance()
                .provisionedResourceId("test-rid2")
                .inProcess(false)
                .build();

        var process1 = createTransferProcessBuilder("test-pid1")
                .deprovisionedResources(List.of(dp1, dp2))
                .build();
        getTransferProcessStore().save(process1);

        var query = QuerySpec.Builder.newInstance()
                .filter(Criterion.criterion("deprovisionedResources.foobar", "=", "barbaz"))
                .build();

        assertThat(getTransferProcessStore().findAll(query)).isEmpty();
    }

    @Test
    void findAll_queryByDeprovisionedResources_valueNotExist() {
        var dp1 = DeprovisionedResource.Builder.newInstance()
                .provisionedResourceId("test-rid1")
                .inProcess(true)
                .errorMessage("not enough resources")
                .build();
        var dp2 = DeprovisionedResource.Builder.newInstance()
                .provisionedResourceId("test-rid2")
                .inProcess(false)
                .errorMessage("undefined error")
                .build();

        var process1 = createTransferProcessBuilder("test-pid1")
                .deprovisionedResources(List.of(dp1, dp2))
                .build();
        getTransferProcessStore().save(process1);

        var query = QuerySpec.Builder.newInstance()
                .filter(Criterion.criterion("deprovisionedResources.errorMessage", "=", "notexist"))
                .build();

        var result = getTransferProcessStore().findAll(query).collect(Collectors.toList());

        assertThat(result).isEmpty();
    }

    @Test
    void findAll_queryByLease() {
        getTransferProcessStore().save(createTransferProcess("testprocess1"));

        var query = QuerySpec.Builder.newInstance()
                .filter(List.of(new Criterion("lease.leasedBy", "=", "foobar")))
                .build();

        assertThat(getTransferProcessStore().findAll(query)).isEmpty();
    }

    @Test
    void findAll_shouldThrowException_whenSortingByNotExistentField() {
        range(0, 10).forEach(i -> getTransferProcessStore().save(createTransferProcess("test-neg-" + i)));

        var query = QuerySpec.Builder.newInstance().sortField("notexist").sortOrder(SortOrder.DESC).build();

        assertThatThrownBy(() -> getTransferProcessStore().findAll(query).toList())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void findByIdAndLease_shouldReturnTheEntityAndLeaseIt() {
        var id = UUID.randomUUID().toString();
        getTransferProcessStore().save(createTransferProcess(id));

        var result = getTransferProcessStore().findByIdAndLease(id);

        AbstractResultAssert.assertThat(result).isSucceeded();
        assertThat(isLeasedBy(id, CONNECTOR_NAME)).isTrue();
    }

    @Test
    void findByIdAndLease_shouldReturnNotFound_whenEntityDoesNotExist() {
        var result = getTransferProcessStore().findByIdAndLease("unexistent");

        AbstractResultAssert.assertThat(result).isFailed().extracting(StoreFailure::getReason).isEqualTo(NOT_FOUND);
    }


    @Test
    void findByIdAndLease_shouldReturnAlreadyLeased_whenEntityIsAlreadyLeased() {
        var id = UUID.randomUUID().toString();
        getTransferProcessStore().save(createTransferProcess(id));
        leaseEntity(id, "other owner");

        var result = getTransferProcessStore().findByIdAndLease(id);

        AbstractResultAssert.assertThat(result).isFailed().extracting(StoreFailure::getReason).isEqualTo(ALREADY_LEASED);
    }

    @Test
    void findByCorrelationIdAndLease_shouldReturnTheEntityAndLeaseIt() {
        var id = UUID.randomUUID().toString();
        var correlationId = UUID.randomUUID().toString();
        getTransferProcessStore().save(createTransferProcessBuilder(id).correlationId(correlationId).build());

        var result = getTransferProcessStore().findByIdAndLease(correlationId);

        AbstractResultAssert.assertThat(result).isSucceeded();
        assertThat(isLeasedBy(id, CONNECTOR_NAME)).isTrue();
    }

    @Test
    void findByCorrelationIdAndLease_shouldReturnNotFound_whenEntityDoesNotExist() {
        var result = getTransferProcessStore().findByIdAndLease("unexistent");

        AbstractResultAssert.assertThat(result).isFailed().extracting(StoreFailure::getReason).isEqualTo(NOT_FOUND);
    }

    @Test
    void findByCorrelationIdAndLease_shouldReturnAlreadyLeased_whenEntityIsAlreadyLeased() {
        var id = UUID.randomUUID().toString();
        var correlationId = UUID.randomUUID().toString();
        getTransferProcessStore().save(createTransferProcessBuilder(id).correlationId(correlationId).build());
        leaseEntity(id, "other owner");

        var result = getTransferProcessStore().findByIdAndLease(correlationId);

        AbstractResultAssert.assertThat(result).isFailed().extracting(StoreFailure::getReason).isEqualTo(ALREADY_LEASED);
    }

    @BeforeAll
    static void createDatabase(GaussDbTestExtension.SqlHelper runner) throws IOException {
        var schema = Files.readString(Paths.get("docs/schema.sql"));
        runner.executeStatement(schema);
    }

    @AfterAll
    static void deleteTable(GaussDbTestExtension.SqlHelper runner) {
        runner.dropTable(SQL_STATEMENTS.getTransferProcessTableName());
        runner.dropTable(SQL_STATEMENTS.getLeaseTableName());
    }

    protected TransferProcessStore getTransferProcessStore() {
        return transferProcessStore;
    }

    protected void leaseEntity(String negotiationId, String owner) {
        leaseEntity(negotiationId, owner, Duration.ofSeconds(60));
    }

    protected void leaseEntity(String negotiationId, String owner, Duration duration) {
        leaseUtil.leaseEntity(negotiationId, owner, duration);
    }

    protected boolean isLeasedBy(String negotiationId, String owner) {
        return leaseUtil.isLeased(negotiationId, owner);
    }

    private void delayByTenMillis(TransferProcess t) {
        try {
            Thread.sleep(10);
        } catch (InterruptedException ignored) {
            // noop
        }
        t.updateStateTimestamp();
    }
}