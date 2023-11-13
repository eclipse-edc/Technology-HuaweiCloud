package com.huawei.cloud.store.gaussdb.contractdefinition;

import org.assertj.core.api.Assertions;
import org.eclipse.edc.azure.testfixtures.GaussDbTestExtension;
import org.eclipse.edc.azure.testfixtures.annotations.GaussDbTest;
import org.eclipse.edc.connector.contract.spi.offer.store.ContractDefinitionStore;
import org.eclipse.edc.connector.contract.spi.types.offer.ContractDefinition;
import org.eclipse.edc.connector.store.sql.contractdefinition.SqlContractDefinitionStore;
import org.eclipse.edc.connector.store.sql.contractdefinition.schema.BaseSqlDialectStatements;
import org.eclipse.edc.junit.assertions.AbstractResultAssert;
import org.eclipse.edc.policy.model.PolicyRegistrationTypes;
import org.eclipse.edc.spi.query.Criterion;
import org.eclipse.edc.spi.query.QuerySpec;
import org.eclipse.edc.spi.query.SortOrder;
import org.eclipse.edc.spi.result.StoreFailure;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.asset.Asset;
import org.eclipse.edc.sql.QueryExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.eclipse.edc.azure.testfixtures.GaussDbTestExtension.DEFAULT_DATASOURCE_NAME;
import static org.eclipse.edc.connector.contract.spi.testfixtures.offer.store.TestFunctions.createContractDefinition;
import static org.eclipse.edc.connector.contract.spi.testfixtures.offer.store.TestFunctions.createContractDefinitions;
import static org.eclipse.edc.junit.testfixtures.TestUtils.getResourceFileContentAsString;
import static org.eclipse.edc.spi.query.Criterion.criterion;
import static org.eclipse.edc.spi.result.StoreFailure.Reason.ALREADY_EXISTS;
import static org.eclipse.edc.spi.result.StoreFailure.Reason.NOT_FOUND;

@GaussDbTest
@ExtendWith(GaussDbTestExtension.class)
class GaussDbContractDefinitionStoreTest {
    private static final BaseSqlDialectStatements SQL_STATEMENTS = new GaussDbStatements();
    private SqlContractDefinitionStore sqlContractDefinitionStore;

    @BeforeEach
    void setUp(GaussDbTestExtension extension, GaussDbTestExtension.SqlHelper helper, QueryExecutor queryExecutor) throws IOException {

        var typeManager = new TypeManager();
        typeManager.registerTypes(PolicyRegistrationTypes.TYPES.toArray(Class<?>[]::new));

        sqlContractDefinitionStore = new SqlContractDefinitionStore(extension.getRegistry(), DEFAULT_DATASOURCE_NAME,
                extension.getTransactionContext(), SQL_STATEMENTS, typeManager.getMapper(), queryExecutor);
        var schema = getResourceFileContentAsString("schema.sql");
        helper.executeStatement(schema);
    }

    @AfterEach
    void tearDown(GaussDbTestExtension.SqlHelper helper) {
        helper.executeStatement("DROP TABLE " + SQL_STATEMENTS.getContractDefinitionTable() + " CASCADE");
    }


    @Test
    @DisplayName("Save a single Contract Definition that doesn't already exist")
    void save_doesNotExist() {
        var definition = createContractDefinition("id");
        getContractDefinitionStore().save(definition);

        var definitions = getContractDefinitionStore().findAll(QuerySpec.max())
                .collect(Collectors.toList());

        assertThat(definitions).hasSize(1);
        assertThat(definitions.get(0)).usingRecursiveComparison().isEqualTo(definition);
    }

    @Test
    @DisplayName("Shouldn't save a single Contract Definition that already exists")
    void save_alreadyExist_shouldNotUpdate() {
        getContractDefinitionStore().save(createContractDefinition("id", "policy", "contract"));
        var saveResult = getContractDefinitionStore().save(createContractDefinition("id", "updatedAccess", "updatedContract"));

        assertThat(saveResult.failed()).isTrue();
        assertThat(saveResult.reason()).isEqualTo(ALREADY_EXISTS);

        var result = getContractDefinitionStore().findAll(QuerySpec.max());

        assertThat(result).hasSize(1).containsExactly(createContractDefinition("id", "policy", "contract"));
    }

    @Test
    @DisplayName("Save a single Contract Definition that is identical to an existing contract definition except for the id")
    void save_sameParametersDifferentId() {
        var definition1 = createContractDefinition("id1", "policy", "contract");
        var definition2 = createContractDefinition("id2", "policy", "contract");
        getContractDefinitionStore().save(definition1);
        getContractDefinitionStore().save(definition2);

        var definitions = getContractDefinitionStore().findAll(QuerySpec.max());

        assertThat(definitions).isNotNull().hasSize(2);
    }

    @Test
    @DisplayName("Save multiple Contract Definitions with no preexisting Definitions")
    void save_noneExist() {
        var definitionsCreated = createContractDefinitions(10);
        saveContractDefinitions(definitionsCreated);

        var definitionsRetrieved = getContractDefinitionStore().findAll(QuerySpec.max());

        assertThat(definitionsRetrieved).hasSize(definitionsCreated.size());
    }

    @Test
    @DisplayName("Save multiple Contract Definitions with some preexisting Definitions")
    void save_someExist() {
        saveContractDefinitions(createContractDefinitions(3));
        saveContractDefinitions(createContractDefinitions(10));

        var definitionsRetrieved = getContractDefinitionStore().findAll(QuerySpec.max());

        assertThat(definitionsRetrieved).hasSize(10);
    }

    @Test
    @DisplayName("Save multiple Contract Definitions with all preexisting Definitions")
    void save_allExist() {
        var definitionsCreated = createContractDefinitions(10);
        saveContractDefinitions(definitionsCreated);
        saveContractDefinitions(definitionsCreated);

        var definitionsRetrieved = getContractDefinitionStore().findAll(QuerySpec.max());

        assertThat(definitionsRetrieved).isNotNull().hasSize(definitionsCreated.size());
    }

    @Test
    @DisplayName("Save a single Contract Definition that doesn't already exist with private properties")
    void save_doesNotExist_with_private_properties() {
        var definition = createContractDefinition("id1", "policy", "contract", Map.of("key1", "value1", "key2", "value2"));
        getContractDefinitionStore().save(definition);

        var definitions = getContractDefinitionStore().findAll(QuerySpec.max())
                .collect(Collectors.toList());

        assertThat(definitions).hasSize(1);
        assertThat(definitions.get(0)).usingRecursiveComparison().isEqualTo(definition);

        assertThat(definitions.get(0).getPrivateProperties()).hasSize(2);
        assertThat(definitions.get(0).getPrivateProperties().get("key1")).usingRecursiveComparison().isEqualTo("value1");
        assertThat(definitions.get(0).getPrivateProperties().get("key2")).usingRecursiveComparison().isEqualTo("value2");
    }

    @Test
    @DisplayName("Update a non-existing Contract Definition")
    void update_doesNotExist_shouldNotCreate() {
        var definition = createContractDefinition("id", "policy1", "contract1");

        var result = getContractDefinitionStore().update(definition);

        assertThat(result.failed()).isTrue();
        assertThat(result.reason()).isEqualTo(NOT_FOUND);

        var existing = getContractDefinitionStore().findAll(QuerySpec.max());

        assertThat(existing).hasSize(0);
    }

    @Test
    @DisplayName("Update an existing Contract Definition")
    void update_exists() {
        var definition1 = createContractDefinition("id", "policy1", "contract1");
        var definition2 = createContractDefinition("id", "policy2", "contract2");

        getContractDefinitionStore().save(definition1);
        getContractDefinitionStore().update(definition2);

        var definitions = getContractDefinitionStore().findAll(QuerySpec.none()).collect(Collectors.toList());

        assertThat(definitions).isNotNull().hasSize(1).first().satisfies(definition -> {
            assertThat(definition.getAccessPolicyId()).isEqualTo(definition2.getAccessPolicyId());
            assertThat(definition.getContractPolicyId()).isEqualTo(definition2.getContractPolicyId());
        });
    }

    @Test
    @DisplayName("Update contract definition that exists, adding a property")
    void update_exists_addsProperty() {
        var definition1 = createContractDefinition("id1", "policy1", "contract1");
        getContractDefinitionStore().save(definition1);
        var definitions = getContractDefinitionStore().findAll(QuerySpec.none()).collect(Collectors.toList());
        assertThat(definitions).isNotNull().hasSize(1);

        definition1.getPrivateProperties().put("newKey", "newValue");
        var updated = getContractDefinitionStore().update(definition1);
        Assertions.assertThat(updated).isNotNull();

        var definitionFound = getContractDefinitionStore().findById("id1");

        assertThat(definitionFound).isNotNull();
        assertThat(definitionFound).usingRecursiveComparison().isEqualTo(definition1);
    }

    @Test
    @DisplayName("Update contract definition that exists, removing a property")
    void update_exists_removesProperty() {
        var definition1 = createContractDefinition("id1", "policy1", "contract1");
        definition1.getPrivateProperties().put("newKey", "newValue");
        getContractDefinitionStore().save(definition1);
        var definitions = getContractDefinitionStore().findAll(QuerySpec.none()).collect(Collectors.toList());
        assertThat(definitions).isNotNull().hasSize(1);

        definition1.getPrivateProperties().remove("newKey");
        var updated = getContractDefinitionStore().update(definition1);
        Assertions.assertThat(updated).isNotNull();

        var definitionFound = getContractDefinitionStore().findById("id1");

        assertThat(definitionFound).isNotNull();
        assertThat(definitionFound).usingRecursiveComparison().isEqualTo(definition1);
        assertThat(definitionFound.getPrivateProperties()).doesNotContainKey("newKey");
    }

    @Test
    @DisplayName("Update an Asset that exists, replacing a property")
    void update_exists_replacingProperty() {
        var definition1 = createContractDefinition("id1", "policy1", "contract1");
        definition1.getPrivateProperties().put("newKey", "originalValue");
        getContractDefinitionStore().save(definition1);
        var definitions = getContractDefinitionStore().findAll(QuerySpec.none()).collect(Collectors.toList());
        assertThat(definitions).isNotNull().hasSize(1);

        definition1.getPrivateProperties().put("newKey", "newValue");
        var updated = getContractDefinitionStore().update(definition1);
        Assertions.assertThat(updated).isNotNull();

        var definitionFound = getContractDefinitionStore().findById("id1");

        assertThat(definitionFound).isNotNull();
        assertThat(definitionFound).usingRecursiveComparison().isEqualTo(definition1);
        assertThat(definitionFound.getPrivateProperties()).containsEntry("newKey", "newValue");
    }

    @Test
    void findAll_shouldReturnAll_whenNoFiltersApplied() {
        var definitionsExpected = createContractDefinitions(10);
        saveContractDefinitions(definitionsExpected);

        var definitionsRetrieved = getContractDefinitionStore().findAll(QuerySpec.max());

        assertThat(definitionsRetrieved).isNotNull().hasSize(definitionsExpected.size());
    }

    @ParameterizedTest
    @ValueSource(ints = {49, 50, 51, 100})
    void findAll_verifyQueryDefaults(int size) {
        var all = IntStream.range(0, size).mapToObj(i -> createContractDefinition("id" + i, "policyId" + i, "contractId" + i))
                .peek(cd -> getContractDefinitionStore().save(cd))
                .collect(Collectors.toList());

        assertThat(getContractDefinitionStore().findAll(QuerySpec.max())).hasSize(size)
                .usingRecursiveFieldByFieldElementComparator()
                .isSubsetOf(all);
    }

    @Test
    @DisplayName("Find all contract definitions with limit and offset")
    void findAll_withSpec() {
        var limit = 20;

        var definitionsExpected = createContractDefinitions(50);
        saveContractDefinitions(definitionsExpected);

        var spec = QuerySpec.Builder.newInstance()
                .limit(limit)
                .offset(20)
                .build();

        var definitionsRetrieved = getContractDefinitionStore().findAll(spec);

        assertThat(definitionsRetrieved).isNotNull().hasSize(limit);
    }

    @Test
    @DisplayName("Find all contract definitions that exactly match a particular access policy ID")
    void findAll_queryByAccessPolicyId_withEquals() {

        var definitionsExpected = createContractDefinitions(20);
        saveContractDefinitions(definitionsExpected);

        var spec = QuerySpec.Builder.newInstance()
                .filter(criterion("accessPolicyId", "=", "policy4"))
                .build();

        var definitionsRetrieved = getContractDefinitionStore().findAll(spec);

        assertThat(definitionsRetrieved).hasSize(1)
                .usingRecursiveFieldByFieldElementComparator()
                .allSatisfy(cd -> assertThat(cd.getId()).isEqualTo("id4"));
    }

    @Test
    @DisplayName("Find all contract definitions that match a range of access policy IDs")
    void findAll_queryByAccessPolicyId_withIn() {

        var definitionsExpected = createContractDefinitions(20);
        saveContractDefinitions(definitionsExpected);

        var spec = QuerySpec.Builder.newInstance()
                .filter(List.of(new Criterion("accessPolicyId", "in", List.of("policy4", "policy5", "policy6"))))
                .build();

        var definitionsRetrieved = getContractDefinitionStore().findAll(spec);

        assertThat(definitionsRetrieved).hasSize(3)
                .usingRecursiveFieldByFieldElementComparator()
                .allMatch(cd -> cd.getId().matches("(id)[4-6]"));
    }

    @Test
    @DisplayName("Verify empty result when query contains a nonexistent value")
    void findAll_queryByNonexistentValue() {

        var definitionsExpected = createContractDefinitions(20);
        saveContractDefinitions(definitionsExpected);

        var spec = QuerySpec.Builder.newInstance()
                .filter(List.of(new Criterion("contractPolicyId", "=", "somevalue")))
                .build();

        assertThat(getContractDefinitionStore().findAll(spec)).isEmpty();
    }

    @Test
    void findAll_invalidOperator() {

        var definitionsExpected = createContractDefinitions(20);
        saveContractDefinitions(definitionsExpected);

        var spec = QuerySpec.Builder.newInstance()
                .filter(List.of(new Criterion("accessPolicyId", "sqrt", "foobar"))) //sqrt is invalid
                .build();

        assertThatThrownBy(() -> getContractDefinitionStore().findAll(spec)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void findAll_defaultQuerySpec() {
        var all = IntStream.range(0, 10).mapToObj(i -> createContractDefinition("id" + i)).peek(getContractDefinitionStore()::save).toList();
        assertThat(getContractDefinitionStore().findAll(QuerySpec.none())).containsExactlyInAnyOrder(all.toArray(new ContractDefinition[] {}));
    }

    @Test
    void findAll_verifyPaging() {
        IntStream.range(0, 10).mapToObj(i -> createContractDefinition("id" + i)).forEach(getContractDefinitionStore()::save);

        // page size fits
        assertThat(getContractDefinitionStore().findAll(QuerySpec.Builder.newInstance().offset(4).limit(2).build())).hasSize(2);

        // page size larger than collection
        assertThat(getContractDefinitionStore().findAll(QuerySpec.Builder.newInstance().offset(5).limit(100).build())).hasSize(5);
    }

    @Test
    void findAll_verifyFiltering() {
        IntStream.range(0, 10).mapToObj(i -> createContractDefinition("id" + i)).forEach(getContractDefinitionStore()::save);
        var criterion = criterion("id", "=", "id3");
        var querySpec = QuerySpec.Builder.newInstance().filter(criterion).build();

        var result = getContractDefinitionStore().findAll(querySpec);

        assertThat(result).extracting(ContractDefinition::getId).containsOnly("id3");
    }

    @Test
    void findAll_shouldReturnEmpty_whenQueryByInvalidKey() {
        var definitionsExpected = createContractDefinitions(5);
        saveContractDefinitions(definitionsExpected);

        var spec = QuerySpec.Builder.newInstance()
                .filter(criterion("not-exist", "=", "some-value"))
                .build();

        assertThat(getContractDefinitionStore().findAll(spec)).isEmpty();
    }

    @Test
    void findAll_verifySorting() {
        IntStream.range(0, 10).mapToObj(i -> createContractDefinition("id" + i)).forEach(getContractDefinitionStore()::save);

        assertThat(getContractDefinitionStore().findAll(QuerySpec.Builder.newInstance().sortField("id").sortOrder(SortOrder.ASC).build())).hasSize(10).isSortedAccordingTo(Comparator.comparing(ContractDefinition::getId));
        assertThat(getContractDefinitionStore().findAll(QuerySpec.Builder.newInstance().sortField("id").sortOrder(SortOrder.DESC).build())).hasSize(10).isSortedAccordingTo((c1, c2) -> c2.getId().compareTo(c1.getId()));
    }

    @Test
    void findAll_verifySorting_invalidProperty() {
        IntStream.range(0, 10).mapToObj(i -> createContractDefinition("id" + i)).forEach(getContractDefinitionStore()::save);
        var query = QuerySpec.Builder.newInstance().sortField("not-exist").sortOrder(SortOrder.DESC).build();

        // must actually collect, otherwise the stream is not materialized
        assertThatThrownBy(() -> getContractDefinitionStore().findAll(query).toList()).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void findAll_queryByAssetsSelector_left() {
        var definitionsExpected = createContractDefinitions(20);
        // add a selector expression to the last 5 elements
        definitionsExpected.get(0).getAssetsSelector().add(new Criterion(Asset.PROPERTY_ID, "=", "test-asset"));
        definitionsExpected.get(5).getAssetsSelector().add(new Criterion(Asset.PROPERTY_ID, "=", "foobar-asset"));
        saveContractDefinitions(definitionsExpected);

        var spec = QuerySpec.Builder.newInstance()
                .filter(criterion("assetsSelector.operandLeft", "=", Asset.PROPERTY_ID))
                .build();

        var definitionsRetrieved = getContractDefinitionStore().findAll(spec);

        assertThat(definitionsRetrieved).hasSize(2)
                .usingRecursiveFieldByFieldElementComparator()
                .allSatisfy(cd -> assertThat(cd.getId()).matches("id[0,5]"));
    }

    @Test
    void findAll_queryByAssetsSelector_right() {
        var definitionsExpected = createContractDefinitions(20);
        definitionsExpected.get(0).getAssetsSelector().add(new Criterion(Asset.PROPERTY_ID, "=", "test-asset"));
        var expectedDef = definitionsExpected.get(5);
        expectedDef.getAssetsSelector().add(new Criterion(Asset.PROPERTY_ID, "=", "foobar-asset"));
        saveContractDefinitions(definitionsExpected);

        var spec = QuerySpec.Builder.newInstance()
                .filter(criterion("assetsSelector.operandRight", "=", "foobar-asset"))
                .build();

        var definitionsRetrieved = getContractDefinitionStore().findAll(spec);

        assertThat(definitionsRetrieved).hasSize(1)
                .usingRecursiveFieldByFieldElementComparator()
                .containsOnly(expectedDef);
    }

    @Test
    void findAll_queryByAssetsSelector_rightAndLeft() {
        var definitionsExpected = createContractDefinitions(10);
        definitionsExpected.get(0).getAssetsSelector().add(new Criterion(Asset.PROPERTY_ID, "=", "test-asset"));
        definitionsExpected.get(5).getAssetsSelector().add(new Criterion(Asset.PROPERTY_ID, "=", "foobar-asset"));
        saveContractDefinitions(definitionsExpected);

        var spec = QuerySpec.Builder.newInstance()
                .filter(List.of(
                        new Criterion("assetsSelector.operandLeft", "=", Asset.PROPERTY_ID),
                        new Criterion("assetsSelector.operandRight", "=", "foobar-asset")))
                .build();

        var definitionsRetrieved = getContractDefinitionStore().findAll(spec);

        assertThat(definitionsRetrieved).hasSize(1)
                .usingRecursiveFieldByFieldElementComparator()
                .containsOnly(definitionsExpected.get(5));
    }

    @Test
    void findAll_queryMultiple() {
        var definitionsExpected = createContractDefinitions(20);
        definitionsExpected.forEach(d -> d.getAssetsSelector().add(new Criterion(Asset.PROPERTY_ID, "=", "test-asset")));
        definitionsExpected.forEach(d -> d.getAssetsSelector().add(new Criterion(Asset.PROPERTY_DESCRIPTION, "=", "other")));
        saveContractDefinitions(definitionsExpected);

        var spec = QuerySpec.Builder.newInstance()
                .filter(new Criterion("assetsSelector.operandRight", "=", "test-asset"))
                .filter(new Criterion("contractPolicyId", "=", "contract4"))
                .build();

        var definitionsRetrieved = getContractDefinitionStore().findAll(spec);

        assertThat(definitionsRetrieved).hasSize(1)
                .usingRecursiveFieldByFieldElementComparator()
                .containsOnly(definitionsExpected.get(4));
    }

    @Test
    void findAll_shouldReturnAll_with_private_properties_whenNoFiltersApplied() {
        var definition1 = createContractDefinition("definition1", "policyId", "contractId", Map.of("key1", "value1"));
        getContractDefinitionStore().save(definition1);
        var definition2 = createContractDefinition("definition2", "policyId", "contractId", Map.of("key2", "value2"));
        getContractDefinitionStore().save(definition2);

        var definitionsRetrieved = getContractDefinitionStore().findAll(QuerySpec.max());
        assertThat(definitionsRetrieved).isNotNull().hasSize(2);
    }

    @Test
    void findById() {
        var id = "definitionId";
        var definition = createContractDefinition(id, "policyId", "contractId");
        getContractDefinitionStore().save(definition);

        var result = getContractDefinitionStore().findById(id);

        assertThat(result).isNotNull().isEqualTo(definition);
    }

    @Test
    void findById_invalidId() {
        assertThat(getContractDefinitionStore().findById("invalid-id")).isNull();
    }

    @Test
    void findById_with_private_properties() {
        var id = "definitionId";
        var definition = createContractDefinition(id, "policyId", "contractId", Map.of("key1", "value1"));
        getContractDefinitionStore().save(definition);

        var result = getContractDefinitionStore().findById(id);

        assertThat(result).isNotNull().isEqualTo(definition);
    }

    @Test
    void delete_shouldDelete() {
        var definitionExpected = createContractDefinition("test-id1", "policy1", "contract1");
        getContractDefinitionStore().save(definitionExpected);
        assertThat(getContractDefinitionStore().findAll(QuerySpec.max())).hasSize(1);

        var deleted = getContractDefinitionStore().deleteById("test-id1");

        assertThat(deleted.succeeded()).isTrue();
        assertThat(deleted.getContent()).isNotNull().usingRecursiveComparison().isEqualTo(definitionExpected);
        assertThat(getContractDefinitionStore().findAll(QuerySpec.max())).isEmpty();
    }

    @Test
    void delete_shouldNotDelete_whenEntityDoesNotExist() {
        var deleted = getContractDefinitionStore().deleteById("test-id1");

        AbstractResultAssert.assertThat(deleted).isFailed().extracting(StoreFailure::getReason).isEqualTo(NOT_FOUND);
    }

    @Test
    void delete_verifyStore() {
        var definition1 = createContractDefinition("1");
        var definition2 = createContractDefinition("2");

        getContractDefinitionStore().save(definition1);
        assertThat(getContractDefinitionStore().findAll(QuerySpec.max())).contains(definition1);

        getContractDefinitionStore().save(definition2);
        assertThat(getContractDefinitionStore().findAll(QuerySpec.max())).contains(definition1);

        var deletedDefinition = getContractDefinitionStore().deleteById(definition1.getId());
        assertThat(deletedDefinition.succeeded()).isTrue();
        assertThat(deletedDefinition.getContent()).isEqualTo(definition1);
        assertThat(getContractDefinitionStore().findAll(QuerySpec.max())).doesNotContain(definition1);
    }

    @Test
    void delete_shouldDelete_with_private_properties() {
        var definitionExpected = createContractDefinition("test-id1", "policy1", "contract1", Map.of("key1", "value1"));
        getContractDefinitionStore().save(definitionExpected);
        assertThat(getContractDefinitionStore().findAll(QuerySpec.max())).hasSize(1);

        var deleted = getContractDefinitionStore().deleteById("test-id1");

        assertThat(deleted.succeeded()).isTrue();
        assertThat(deleted.getContent()).isNotNull().usingRecursiveComparison().isEqualTo(definitionExpected);
        assertThat(getContractDefinitionStore().findAll(QuerySpec.max())).isEmpty();
    }

    protected ContractDefinitionStore getContractDefinitionStore() {
        return sqlContractDefinitionStore;
    }

    protected void saveContractDefinitions(List<ContractDefinition> definitions) {
        definitions.forEach((it) -> {
            this.getContractDefinitionStore().save(it);
        });
    }
}