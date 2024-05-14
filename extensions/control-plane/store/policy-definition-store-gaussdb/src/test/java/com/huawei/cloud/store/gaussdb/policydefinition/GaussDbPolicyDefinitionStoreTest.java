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

package com.huawei.cloud.store.gaussdb.policydefinition;

import com.huawei.cloud.gaussdb.testfixtures.GaussDbTestExtension;
import com.huawei.cloud.gaussdb.testfixtures.annotations.GaussDbTest;
import org.eclipse.edc.connector.controlplane.policy.spi.PolicyDefinition;
import org.eclipse.edc.connector.controlplane.policy.spi.store.PolicyDefinitionStore;
import org.eclipse.edc.connector.controlplane.store.sql.policydefinition.store.SqlPolicyDefinitionStore;
import org.eclipse.edc.connector.controlplane.store.sql.policydefinition.store.schema.BaseSqlDialectStatements;
import org.eclipse.edc.json.JacksonTypeManager;
import org.eclipse.edc.policy.model.Action;
import org.eclipse.edc.policy.model.Duty;
import org.eclipse.edc.policy.model.Permission;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.policy.model.PolicyRegistrationTypes;
import org.eclipse.edc.policy.model.Prohibition;
import org.eclipse.edc.spi.query.Criterion;
import org.eclipse.edc.spi.query.QuerySpec;
import org.eclipse.edc.spi.query.SortOrder;
import org.eclipse.edc.spi.result.StoreResult;
import org.eclipse.edc.sql.QueryExecutor;
import org.eclipse.edc.transaction.datasource.spi.DataSourceRegistry;
import org.eclipse.edc.transaction.spi.TransactionContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.huawei.cloud.gaussdb.testfixtures.GaussDbTestExtension.DEFAULT_DATASOURCE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.eclipse.edc.connector.controlplane.policy.spi.testfixtures.TestFunctions.createAction;
import static org.eclipse.edc.connector.controlplane.policy.spi.testfixtures.TestFunctions.createDutyBuilder;
import static org.eclipse.edc.connector.controlplane.policy.spi.testfixtures.TestFunctions.createPermissionBuilder;
import static org.eclipse.edc.connector.controlplane.policy.spi.testfixtures.TestFunctions.createPolicies;
import static org.eclipse.edc.connector.controlplane.policy.spi.testfixtures.TestFunctions.createPolicy;
import static org.eclipse.edc.connector.controlplane.policy.spi.testfixtures.TestFunctions.createPolicyBuilder;
import static org.eclipse.edc.connector.controlplane.policy.spi.testfixtures.TestFunctions.createProhibitionBuilder;
import static org.eclipse.edc.spi.query.Criterion.criterion;
import static org.eclipse.edc.spi.result.StoreFailure.Reason.ALREADY_EXISTS;
import static org.eclipse.edc.spi.result.StoreFailure.Reason.NOT_FOUND;

@GaussDbTest
@ExtendWith(GaussDbTestExtension.class)
class GaussDbPolicyDefinitionStoreTest {
    private static final BaseSqlDialectStatements SQL_STATEMENTS = new GaussDbStatements();
    private PolicyDefinitionStore policyDefinitionStore;

    @BeforeAll
    static void prepare(GaussDbTestExtension.SqlHelper runner) throws IOException {
        runner.executeStatement(Files.readString(Paths.get("docs/schema.sql")));
    }

    @AfterAll
    static void deleteTable(GaussDbTestExtension.SqlHelper runner) {
        runner.dropTable(SQL_STATEMENTS.getPolicyTable());
    }

    @BeforeEach
    void setup(GaussDbTestExtension.SqlHelper runner, TransactionContext transactionContext, QueryExecutor queryExecutor, DataSourceRegistry reg) {
        var typeManager = new JacksonTypeManager();
        typeManager.registerTypes(PolicyRegistrationTypes.TYPES.toArray(Class<?>[]::new));

        policyDefinitionStore = new SqlPolicyDefinitionStore(reg, DEFAULT_DATASOURCE_NAME, transactionContext, typeManager.getMapper(), SQL_STATEMENTS, queryExecutor);

        runner.truncateTable(SQL_STATEMENTS.getPolicyTable());
    }

    @Test
    @DisplayName("Save a single policy that not exists ")
    void create_notExisting() {
        var policy = createPolicy(getRandomId());

        getPolicyDefinitionStore().create(policy);

        var policyFromDb = getPolicyDefinitionStore().findById(policy.getId());
        assertThat(policy).usingRecursiveComparison().isEqualTo(policyFromDb);
    }

    @Test
    @DisplayName("Save (update) a single policy that already exists")
    void create_alreadyExists() {
        var id = getRandomId();
        var policy1 = PolicyDefinition.Builder.newInstance()
                .policy(Policy.Builder.newInstance()
                        .target("Target1")
                        .build())
                .id(id)
                .build();
        var policy2 = PolicyDefinition.Builder.newInstance()
                .policy(Policy.Builder.newInstance()
                        .target("Target2")
                        .build())
                .id(id)
                .build();
        var spec = QuerySpec.Builder.newInstance().build();

        var store = getPolicyDefinitionStore();
        store.create(policy1);

        var result = store.create(policy2);
        assertThat(result.succeeded()).isFalse();
        assertThat(result.reason()).isEqualTo(ALREADY_EXISTS);

        var policyFromDb = store.findAll(spec);

        assertThat(policyFromDb).hasSize(1).first()
                .satisfies(policy -> assertThat(policy.getPolicy().getTarget()).isEqualTo("Target1"))
                .extracting(PolicyDefinition::getCreatedAt).isEqualTo(policy1.getCreatedAt());
    }

    @Test
    @DisplayName("Update Policy that does not yet exist")
    void update_policyDoesNotExist() {
        var id = getRandomId();
        var policy = createPolicyDef(id, "target");

        var updated = getPolicyDefinitionStore().update(policy);
        assertThat(updated).isNotNull().extracting(StoreResult::succeeded).isEqualTo(false);
        assertThat(updated.reason()).isEqualTo(NOT_FOUND);
    }

    @Test
    @DisplayName("Update an Policy that exists, adding a property")
    void update_policyExists() {
        var id = getRandomId();
        var policy = createPolicyDef(id, "target");

        var store = getPolicyDefinitionStore();
        store.create(policy);

        var newPolicy = createPolicy(id, "target2");
        var result = store.update(newPolicy);

        assertThat(result.succeeded()).isTrue();

        var spec = QuerySpec.Builder.newInstance().build();
        var policyFromDb = store.findAll(spec);

        assertThat(policyFromDb).hasSize(1).first();
        assertThat(result.getContent().getPolicy().getTarget()).isEqualTo("target2");
        assertThat(result.getContent()).usingRecursiveComparison().isEqualTo(newPolicy);
    }

    @Test
    void update_whenPolicyNotExists() {
        var updated = createPolicy("another-id");

        var result = getPolicyDefinitionStore().update(updated);

        assertThat(result).extracting(StoreResult::reason).isEqualTo(NOT_FOUND);
    }

    @Test
    void update_whenPolicyExists_updatingPolicyFields() {
        var policy = createPolicy("test-id");
        var store = getPolicyDefinitionStore();
        store.create(policy);

        var action = Action.Builder.newInstance().type("UPDATED_USE").build();
        var updatedPermission = Permission.Builder.newInstance().action(action).build();
        var updatedDuty = Duty.Builder.newInstance().action(action).build();
        var updatedProhibition = Prohibition.Builder.newInstance().action(action).build();

        policy.getPolicy().getPermissions().add(updatedPermission);
        policy.getPolicy().getProhibitions().add(updatedProhibition);
        policy.getPolicy().getObligations().add(updatedDuty);
        policy.getPolicy().getExtensibleProperties().put("updatedKey", "updatedValue");

        var result = store.update(policy);
        assertThat(result.succeeded()).isTrue();
        assertThat(result.getContent()).isEqualTo(policy);

        var updatedPolicy = result.getContent().getPolicy();
        assertThat(updatedPolicy.getExtensibleProperties()).containsEntry("updatedKey", "updatedValue");
        assertThat(updatedPolicy.getPermissions().get(1)).isEqualTo(updatedPermission);
        assertThat(updatedPolicy.getProhibitions().get(1)).isEqualTo(updatedProhibition);
        assertThat(updatedPolicy.getObligations().get(1)).isEqualTo(updatedDuty);
    }

    @Test
    void update_whenPolicyExists_removingPolicyFields() {
        var policy = createPolicy("test-id");
        getPolicyDefinitionStore().create(policy);

        var updatedPolicy = createPolicyDef("test-id", "updatedTarget");
        var result = getPolicyDefinitionStore().update(updatedPolicy);

        assertThat(result.succeeded()).isTrue();

        var content = result.getContent().getPolicy();
        assertThat(result.getContent()).isEqualTo(updatedPolicy);
        assertThat(content.getTarget()).isEqualTo("updatedTarget");
        assertThat(content.getPermissions().size()).isEqualTo(0);
        assertThat(content.getProhibitions().size()).isEqualTo(0);
        assertThat(content.getObligations().size()).isEqualTo(0);
    }

    @Test
    void update_whenPolicyExists_replacingAllFields() {
        var policyId = "test-id";
        var policy = createPolicy(policyId);
        var store = getPolicyDefinitionStore();
        store.create(policy);

        var updatedPermission = createPermissionBuilder("updated-id").build();
        var updatedProhibition = createProhibitionBuilder("updated-id").build();
        var updatedDuty = createDutyBuilder("updated-id").build();
        var updatedPolicy = Policy.Builder.newInstance()
                .target("updatedTarget")
                .permission(updatedPermission)
                .prohibition(updatedProhibition)
                .duty(updatedDuty)
                .extensibleProperty("updatedKey", "updatedValue")
                .build();

        var result = store.update(PolicyDefinition.Builder.newInstance().id(policyId).policy(updatedPolicy).build());
        assertThat(result.succeeded()).isTrue();
        var content = result.getContent().getPolicy();
        assertThat(content).isEqualTo(updatedPolicy);

        assertThat(content.getExtensibleProperties()).containsEntry("updatedKey", "updatedValue");
        assertThat(content.getPermissions().get(0)).isEqualTo(updatedPermission);
        assertThat(content.getProhibitions().get(0)).isEqualTo(updatedProhibition);
        assertThat(content.getObligations().get(0)).isEqualTo(updatedDuty);
    }

    @Test
    @DisplayName("Find policy by ID that exists")
    void findById_whenPresent() {
        var policy = createPolicy(getRandomId());
        getPolicyDefinitionStore().create(policy);

        var policyFromDb = getPolicyDefinitionStore().findById(policy.getId());

        assertThat(policy).usingRecursiveComparison().isEqualTo(policyFromDb);
    }

    @Test
    @DisplayName("Find policy by ID when not exists")
    void findById_whenNonexistent() {
        assertThat(getPolicyDefinitionStore().findById("nonexistent")).isNull();
    }


    @Test
    @DisplayName("Find all policies with limit and offset")
    void findAll_withSpec() {
        var limit = 20;

        var definitionsExpected = createPolicies(50);
        definitionsExpected.forEach(getPolicyDefinitionStore()::create);

        var spec = QuerySpec.Builder.newInstance()
                .limit(limit)
                .offset(20)
                .build();

        var policiesFromDb = getPolicyDefinitionStore().findAll(spec);

        assertThat(policiesFromDb).hasSize(limit);
    }

    @Test
    @DisplayName("Find policies when page size larger than DB collection")
    void findAll_pageSizeLargerThanDbCollection() {
        var pageSize = 15;

        var definitionsExpected = createPolicies(10);
        definitionsExpected.forEach(getPolicyDefinitionStore()::create);

        var spec = QuerySpec.Builder.newInstance()
                .offset(pageSize)
                .build();

        var policiesFromDb = getPolicyDefinitionStore().findAll(spec);

        assertThat(policiesFromDb).isEmpty();
    }

    @Test
    @DisplayName("Find policies when page size oversteps DB collection size")
    void findAll_pageSizeLarger() {
        var limit = 5;

        var definitionsExpected = createPolicies(10);
        definitionsExpected.forEach(getPolicyDefinitionStore()::create);

        var spec = QuerySpec.Builder.newInstance()
                .offset(7)
                .limit(limit)
                .build();

        var policiesFromDb = getPolicyDefinitionStore().findAll(spec);

        assertThat(policiesFromDb).size().isLessThanOrEqualTo(limit);
    }

    @Test
    void findAll_shouldReturnEmpty_whenPropertyDoesNotExist() {
        var policy = createPolicyBuilder("test-policy")
                .assigner("test-assigner")
                .assignee("test-assignee")
                .build();
        var policyDef1 = PolicyDefinition.Builder.newInstance().id("test-policy").policy(policy).build();
        getPolicyDefinitionStore().create(policyDef1);

        var criterion = criterion("notexist", "=", "foobar");
        var querySpec = QuerySpec.Builder.newInstance().filter(criterion).build();

        var result = getPolicyDefinitionStore().findAll(querySpec);

        assertThat(result).isEmpty();
    }

    @Test
    void findAll_sorting_nonExistentProperty() {
        IntStream.range(0, 10).mapToObj(i -> createPolicy("test-policy")).forEach((d) -> getPolicyDefinitionStore().create(d));

        var query = QuerySpec.Builder.newInstance().sortField("notexist").sortOrder(SortOrder.DESC).build();

        assertThatThrownBy(() -> getPolicyDefinitionStore().findAll(query).toList())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void findAll_queryByProhibitions() {
        var p = createPolicyBuilder("test-policy")
                .prohibition(createProhibitionBuilder("prohibition1")
                        .action(createAction("test-action-type"))
                        .build())
                .build();

        var policyDef = PolicyDefinition.Builder.newInstance().id("test-policy").policy(p).build();
        getPolicyDefinitionStore().create(policyDef);

        // query by prohibition assignee
        var query = createQuery(Criterion.criterion("policy.prohibitions.assignee", "=", "test-assignee"));
        var result = getPolicyDefinitionStore().findAll(query);
        assertThat(result).hasSize(1)
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactly(policyDef);

        //query by prohibition action constraint
        var query2 = createQuery(Criterion.criterion("policy.prohibitions.action.constraint.leftExpression.value", "=", "foo"));
        var result2 = getPolicyDefinitionStore().findAll(query2);
        assertThat(result2).hasSize(1)
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactly(policyDef);
    }

    @Test
    void findAll_queryByProhibitions_valueNotExist() {
        var p = createPolicyBuilder("test-policy")
                .prohibition(createProhibitionBuilder("prohibition1")
                        .action(createAction("test-action-type"))
                        .build())
                .build();

        var policyDef = PolicyDefinition.Builder.newInstance().id("test-policy").policy(p).build();
        getPolicyDefinitionStore().create(policyDef);

        // query by prohibition assignee
        var query = createQuery(Criterion.criterion("policy.prohibitions.action.constraint.leftExpression.value", "=", "someval"));
        var result = getPolicyDefinitionStore().findAll(query);
        assertThat(result).isEmpty();
    }

    @Test
    void findAll_queryByPermissions() {
        var p = createPolicyBuilder("test-policy")
                .permission(createPermissionBuilder("permission1")
                        .action(createAction("test-action-type"))
                        .build())
                .build();

        var policyDef = PolicyDefinition.Builder.newInstance().id("test-policy").policy(p).build();
        getPolicyDefinitionStore().create(policyDef);

        // query by prohibition assignee
        var query = createQuery(Criterion.criterion("policy.permissions.assignee", "=", "test-assignee"));
        var result = getPolicyDefinitionStore().findAll(query);
        assertThat(result).hasSize(1)
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactly(policyDef);

        //query by prohibition action constraint
        var query2 = createQuery(Criterion.criterion("policy.permissions.action.constraint.leftExpression.value", "=", "foo"));
        var result2 = getPolicyDefinitionStore().findAll(query2);
        assertThat(result2).hasSize(1)
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactly(policyDef);
    }

    @Test
    void findAll_queryByPermissions_valueNotExist() {
        var p = createPolicyBuilder("test-policy")
                .permission(createPermissionBuilder("permission1")
                        .action(createAction("test-action-type"))
                        .build())
                .build();

        var policyDef = PolicyDefinition.Builder.newInstance().id("test-policy").policy(p).build();
        getPolicyDefinitionStore().create(policyDef);

        // query by prohibition assignee
        var query = createQuery(Criterion.criterion("policy.permissions.action.constraint.leftExpression", "=", "someval"));
        var result = getPolicyDefinitionStore().findAll(query);
        assertThat(result).isEmpty();
    }

    @Test
    void findAll_queryByDuties() {
        var p = createPolicyBuilder("test-policy")
                .duty(createDutyBuilder("prohibition1")
                        .action(createAction("test-action-type"))
                        .build())
                .build();

        var policyDef = PolicyDefinition.Builder.newInstance().id("test-policy").policy(p).build();
        getPolicyDefinitionStore().create(policyDef);
        getPolicyDefinitionStore().create(createPolicy("another-policy"));

        // query by prohibition assignee
        var query = createQuery(Criterion.criterion("policy.obligations.assignee", "=", "test-assignee"));
        var result = getPolicyDefinitionStore().findAll(query);
        assertThat(result).hasSize(1)
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactly(policyDef);

        //query by prohibition action constraint
        var query2 = createQuery(Criterion.criterion("policy.obligations.action.constraint.rightExpression.value", "=", "bar"));
        var result2 = getPolicyDefinitionStore().findAll(query2);
        assertThat(result2).hasSize(1)
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactly(policyDef);
    }

    @Test
    void findAll_queryByDuties_valueNotExist() {
        var p = createPolicyBuilder("test-policy")
                .duty(createDutyBuilder("prohibition1")
                        .action(createAction("test-action-type"))
                        .build())
                .build();

        var policyDef = PolicyDefinition.Builder.newInstance().id("test-policy").policy(p).build();
        getPolicyDefinitionStore().create(policyDef);

        // query by prohibition assignee
        var query = createQuery(Criterion.criterion("policy.obligations.action.constraint.rightExpression.value", "=", "notexist"));
        var result = getPolicyDefinitionStore().findAll(query);
        assertThat(result).isEmpty();
    }

    @Test
    void findAll_queryByProperty() {
        var p1 = createPolicyBuilder("test-policy")
                .assigner("test-assigner")
                .assignee("test-assignee")
                .build();

        var policyDef1 = PolicyDefinition.Builder.newInstance().id("test-policy").policy(p1).build();
        var p2 = createPolicyBuilder("test-policy")
                .assigner("another-test-assigner")
                .assignee("another-test-assignee")
                .build();

        var policyDef2 = PolicyDefinition.Builder.newInstance().id("test-policy2").policy(p2).build();
        getPolicyDefinitionStore().create(policyDef1);
        getPolicyDefinitionStore().create(policyDef2);

        // query by prohibition assignee
        assertThat(getPolicyDefinitionStore().findAll(createQuery(Criterion.criterion("policy.assignee", "=", "test-assignee"))))
                .hasSize(1)
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactly(policyDef1);
    }

    @Test
    void findAll_queryByProperty_valueNotFound() {
        var policy = createPolicyBuilder("test-policy")
                .assigner("test-assigner")
                .assignee("test-assignee")
                .build();

        var policyDef1 = PolicyDefinition.Builder.newInstance().id("test-policy").policy(policy).build();
        getPolicyDefinitionStore().create(policyDef1);

        // query by prohibition assignee
        var query = createQuery(Criterion.criterion("policy.assigner", "=", "notexist"));

        assertThat(getPolicyDefinitionStore().findAll(query)).isEmpty();
    }

    @Test
    void findAll_whenNoFiltersWithLimitAndOffset() {
        var policy1 = createPolicy(getRandomId());
        var policy2 = createPolicy(getRandomId());
        var policy3 = createPolicy(getRandomId());
        getPolicyDefinitionStore().create(policy1);
        getPolicyDefinitionStore().create(policy2);
        getPolicyDefinitionStore().create(policy3);

        var list = getPolicyDefinitionStore().findAll(QuerySpec.Builder.newInstance().limit(3).offset(1).build()).collect(Collectors.toList());
        assertThat(list).hasSize(2).usingRecursiveFieldByFieldElementComparator().isSubsetOf(policy1, policy2, policy3);
    }

    @Test
    void findAll_whenEqualFilter() {
        var policy1 = createPolicy(getRandomId());
        var policy2 = createPolicy(getRandomId());
        var policy3 = createPolicy(getRandomId());
        getPolicyDefinitionStore().create(policy1);
        getPolicyDefinitionStore().create(policy2);
        getPolicyDefinitionStore().create(policy3);

        var querySpec = QuerySpec.Builder.newInstance().filter(Criterion.criterion("id", "=", policy1.getId())).build();

        assertThat(getPolicyDefinitionStore().findAll(querySpec)).usingRecursiveFieldByFieldElementComparator().containsExactly(policy1);
    }

    @Test
    void findAll_whenSort() {
        var policy1 = createPolicy("C");
        var policy2 = createPolicy("A");
        var policy3 = createPolicy("B");

        getPolicyDefinitionStore().create(policy1);
        getPolicyDefinitionStore().create(policy2);
        getPolicyDefinitionStore().create(policy3);

        assertThat(getPolicyDefinitionStore().findAll(QuerySpec.Builder.newInstance().sortField("id").sortOrder(SortOrder.ASC).build())).usingRecursiveFieldByFieldElementComparator().containsExactly(policy2, policy3, policy1);
    }

    @Test
    void findAll_allFilters() {
        var policy1 = createPolicy("1C", "target1");
        var policy2 = createPolicy("1A", "target1");
        var policy3 = createPolicy("1B", "target1");
        var policyX = createPolicy("2X", "target2");
        var policyY = createPolicy("2Y", "target2");
        getPolicyDefinitionStore().create(policy1);
        getPolicyDefinitionStore().create(policy2);
        getPolicyDefinitionStore().create(policy3);
        getPolicyDefinitionStore().create(policyX);
        getPolicyDefinitionStore().create(policyY);

        var querySpec = QuerySpec.Builder.newInstance()
                .filter(Criterion.criterion("policy.target", "=", "target1"))
                .sortField("id")
                .sortOrder(SortOrder.DESC)
                .offset(1)
                .limit(1)
                .build();

        var result = getPolicyDefinitionStore().findAll(querySpec);

        assertThat(result).usingRecursiveFieldByFieldElementComparator().containsExactly(policy3);
    }

    @Test
    @DisplayName("Delete existing policy")
    void delete_whenExists() {
        var policy = createPolicy(getRandomId());
        var store = getPolicyDefinitionStore();
        store.create(policy);

        var result = store.delete(policy.getId());
        assertThat(result.succeeded()).isTrue();
        assertThat(result.getContent()).usingRecursiveComparison().isEqualTo(policy);
        assertThat(store.findById(policy.getId())).isNull();
    }

    @Test
    @DisplayName("Delete a non existing policy")
    void delete_whenNonexistent() {
        assertThat(getPolicyDefinitionStore().delete("nonexistent"))
                .isNotNull()
                .extracting(StoreResult::reason)
                .isEqualTo(NOT_FOUND);
    }

    protected PolicyDefinitionStore getPolicyDefinitionStore() {
        return policyDefinitionStore;
    }

    private String getRandomId() {
        return UUID.randomUUID().toString();
    }

    private PolicyDefinition createPolicyDef(String id, String target) {
        return PolicyDefinition.Builder.newInstance().id(id).policy(Policy.Builder.newInstance().target(target).build()).build();
    }

    private QuerySpec createQuery(Criterion criterion) {
        return QuerySpec.Builder.newInstance().filter(criterion).build();
    }
}