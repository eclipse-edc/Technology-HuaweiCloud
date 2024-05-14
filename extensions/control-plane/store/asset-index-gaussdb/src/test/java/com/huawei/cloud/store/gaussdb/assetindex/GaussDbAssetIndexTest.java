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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.huawei.cloud.gaussdb.testfixtures.GaussDbTestExtension;
import com.huawei.cloud.gaussdb.testfixtures.annotations.GaussDbTest;
import org.assertj.core.api.Assertions;
import org.eclipse.edc.connector.controlplane.asset.spi.domain.Asset;
import org.eclipse.edc.connector.controlplane.asset.spi.index.AssetIndex;
import org.eclipse.edc.connector.controlplane.store.sql.assetindex.SqlAssetIndex;
import org.eclipse.edc.connector.controlplane.store.sql.assetindex.schema.BaseSqlDialectStatements;
import org.eclipse.edc.connector.controlplane.store.sql.assetindex.schema.postgres.PostgresDialectStatements;
import org.eclipse.edc.json.JacksonTypeManager;
import org.eclipse.edc.policy.model.PolicyRegistrationTypes;
import org.eclipse.edc.spi.query.Criterion;
import org.eclipse.edc.spi.query.QuerySpec;
import org.eclipse.edc.spi.query.SortOrder;
import org.eclipse.edc.spi.result.StoreResult;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.sql.QueryExecutor;
import org.eclipse.edc.transaction.datasource.spi.DataSourceRegistry;
import org.eclipse.edc.transaction.spi.TransactionContext;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.huawei.cloud.gaussdb.testfixtures.GaussDbTestExtension.DEFAULT_DATASOURCE_NAME;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.eclipse.edc.spi.constants.CoreConstants.EDC_NAMESPACE;
import static org.eclipse.edc.spi.query.Criterion.criterion;
import static org.eclipse.edc.spi.result.StoreFailure.Reason.ALREADY_EXISTS;
import static org.eclipse.edc.spi.result.StoreFailure.Reason.NOT_FOUND;

@GaussDbTest
@ExtendWith(GaussDbTestExtension.class)
class GaussDbAssetIndexTest {
    private static final BaseSqlDialectStatements SQL_STATEMENTS = new PostgresDialectStatements();
    private SqlAssetIndex sqlAssetIndex;

    @BeforeEach
    void setup(GaussDbTestExtension.SqlHelper runner, TransactionContext transactionContext, QueryExecutor queryExecutor, DataSourceRegistry reg) {
        var typeManager = new JacksonTypeManager();
        typeManager.registerTypes(PolicyRegistrationTypes.TYPES.toArray(Class<?>[]::new));

        sqlAssetIndex = new SqlAssetIndex(reg, DEFAULT_DATASOURCE_NAME, transactionContext, new ObjectMapper(), SQL_STATEMENTS, queryExecutor);

        runner.truncateTable(SQL_STATEMENTS.getAssetTable());
    }

    @Test
    @DisplayName("Update Asset that does not yet exist")
    void update_doesNotExist() {
        var id = "id1";
        var assetExpected = getAsset(id);
        var assetIndex = getAssetIndex();

        var updated = assetIndex.updateAsset(assetExpected);
        Assertions.assertThat(updated).isNotNull().extracting(StoreResult::succeeded).isEqualTo(false);
    }

    @Test
    @DisplayName("Update an Asset that exists, adding a property")
    void update_exists_addsProperty() {
        var id = "id1";
        var asset = getAsset(id);
        var assetIndex = getAssetIndex();
        assetIndex.create(asset);

        assertThat(assetIndex.countAssets(List.of())).isEqualTo(1);

        asset.getProperties().put("newKey", "newValue");
        var updated = assetIndex.updateAsset(asset);

        Assertions.assertThat(updated).isNotNull();

        var assetFound = getAssetIndex().findById("id1");

        assertThat(assetFound).isNotNull();
        assertThat(assetFound).usingRecursiveComparison().isEqualTo(asset);
    }

    @Test
    @DisplayName("Update an Asset that exists, removing a property")
    void update_exists_removesProperty() {
        var id = "id1";
        var asset = getAsset(id);
        asset.getProperties().put("newKey", "newValue");
        var assetIndex = getAssetIndex();
        assetIndex.create(asset);

        assertThat(assetIndex.countAssets(List.of())).isEqualTo(1);

        asset.getProperties().remove("newKey");
        var updated = assetIndex.updateAsset(asset);

        Assertions.assertThat(updated).isNotNull();

        var assetFound = getAssetIndex().findById("id1");

        assertThat(assetFound).isNotNull();
        assertThat(assetFound).usingRecursiveComparison().isEqualTo(asset);
        assertThat(assetFound.getProperties().keySet()).doesNotContain("newKey");
    }

    @Test
    @DisplayName("Update an Asset that exists, replacing a property")
    void update_exists_replacingProperty() {
        var id = "id1";
        var asset = getAsset(id);
        asset.getProperties().put("newKey", "originalValue");
        var assetIndex = getAssetIndex();
        assetIndex.create(asset);

        assertThat(assetIndex.countAssets(List.of())).isEqualTo(1);

        asset.getProperties().put("newKey", "newValue");
        var updated = assetIndex.updateAsset(asset);

        Assertions.assertThat(updated).isNotNull();

        var assetFound = getAssetIndex().findById("id1");

        assertThat(assetFound).isNotNull();
        assertThat(assetFound).usingRecursiveComparison().isEqualTo(asset);
        assertThat(assetFound.getProperties()).containsEntry("newKey", "newValue");
    }

    @Test
    void update_exists_updateDataAddress() {
        var id = "id1";
        var asset = getAsset(id);
        var assetIndex = getAssetIndex();
        assetIndex.create(asset);

        assertThat(assetIndex.countAssets(List.of())).isEqualTo(1);

        asset.getDataAddress().getProperties().put("newKey", "newValue");
        var updated = assetIndex.updateAsset(asset);

        Assertions.assertThat(updated).isNotNull();

        var assetFound = getAssetIndex().findById("id1");

        assertThat(assetFound).isNotNull();
        assertThat(assetFound).usingRecursiveComparison().isEqualTo(asset);
    }

    @Test
    void create_shouldStoreAsset() {
        var assetExpected = getAsset("id1");
        getAssetIndex().create(assetExpected);

        var assetFound = getAssetIndex().findById("id1");

        assertThat(assetFound).isNotNull();
        assertThat(assetFound).usingRecursiveComparison().isEqualTo(assetExpected);
        assertThat(assetFound.getCreatedAt()).isGreaterThan(0);
    }

    @Test
    void create_shouldFail_whenAssetAlreadyExists() {
        var asset = createAsset("test-asset", UUID.randomUUID().toString());
        var assetIndex = getAssetIndex();
        assetIndex.create(asset);

        var result = assetIndex.create(asset);

        assertThat(result.succeeded()).isFalse();
        assertThat(result.reason()).isEqualTo(ALREADY_EXISTS);
        //assert that this replaces the previous data address
        assertThat(getAssetIndex().queryAssets(QuerySpec.none())).hasSize(1)
                .usingRecursiveFieldByFieldElementComparator()
                .contains(asset);
    }

    @Test
    @DisplayName("Delete an asset that doesn't exist")
    void delete_doesNotExist() {
        var assetDeleted = getAssetIndex().deleteById("id1");

        Assertions.assertThat(assetDeleted).isNotNull().extracting(StoreResult::reason).isEqualTo(NOT_FOUND);
    }

    @Test
    @DisplayName("Delete an asset that exists")
    void delete_exists() {
        var asset = getAsset("id1");
        getAssetIndex().create(asset);

        var assetDeleted = getAssetIndex().deleteById("id1");

        Assertions.assertThat(assetDeleted).isNotNull().extracting(StoreResult::succeeded).isEqualTo(true);
        assertThat(assetDeleted.getContent()).usingRecursiveComparison().isEqualTo(asset);

        assertThat(getAssetIndex().queryAssets(QuerySpec.none())).isEmpty();
    }

    @Test
    void count_withResults() {
        var assets = range(0, 5).mapToObj(i -> getAsset("id" + i));
        assets.forEach(a -> getAssetIndex().create(a));
        var criteria = Collections.<Criterion>emptyList();

        var count = getAssetIndex().countAssets(criteria);

        assertThat(count).isEqualTo(5);
    }

    @Test
    void count_withNoResults() {
        var criteria = Collections.<Criterion>emptyList();

        var count = getAssetIndex().countAssets(criteria);

        assertThat(count).isEqualTo(0);
    }

    @Test
    void query_shouldReturnAllTheAssets_whenQuerySpecIsEmpty() {
        var assets = IntStream.range(0, 5)
                .mapToObj(i -> createAsset("test-asset", "id" + i))
                .peek(a -> getAssetIndex().create(a)).toList();

        var result = getAssetIndex().queryAssets(QuerySpec.none());

        var result1 = result.toList();
        assertThat(result1).hasSize(5).usingRecursiveFieldByFieldElementComparator().containsAll(assets);
    }

    @Test
    @DisplayName("Query assets with query spec")
    void query_limit() {
        for (var i = 1; i <= 10; i++) {
            var asset = getAsset("id" + i);
            getAssetIndex().create(asset);
        }
        var querySpec = QuerySpec.Builder.newInstance().limit(3).offset(2).build();

        var assetsFound = getAssetIndex().queryAssets(querySpec);

        assertThat(assetsFound).isNotNull().hasSize(3);
    }

    @Test
    @DisplayName("Query assets with query spec and short asset count")
    void query_shortCount() {
        range(1, 5).mapToObj(it -> getAsset("id" + it)).forEach(asset -> getAssetIndex().create(asset));
        var querySpec = QuerySpec.Builder.newInstance()
                .limit(3)
                .offset(2)
                .build();

        var assetsFound = getAssetIndex().queryAssets(querySpec);

        assertThat(assetsFound).isNotNull().hasSize(2);
    }

    @Test
    void query_shouldReturnNoAssets_whenOffsetIsOutOfBounds() {
        range(1, 5).mapToObj(it -> getAsset("id" + it)).forEach(asset -> getAssetIndex().create(asset));
        var querySpec = QuerySpec.Builder.newInstance()
                .limit(3)
                .offset(5)
                .build();

        var assetsFound = getAssetIndex().queryAssets(querySpec);

        assertThat(assetsFound).isEmpty();
    }

    @Test
    void query_shouldThrowException_whenUnsupportedOperator() {
        var asset = getAsset("id1");
        getAssetIndex().create(asset);
        var unsupportedOperator = new Criterion(Asset.PROPERTY_ID, "unsupported", "42");

        assertThatThrownBy(() -> getAssetIndex().queryAssets(filter(unsupportedOperator)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void query_shouldReturnEmpty_whenLeftOperandDoesNotExist() {
        var asset = getAsset("id1");
        getAssetIndex().create(asset);
        var notExistingProperty = new Criterion("noexist", "=", "42");

        var result = getAssetIndex().queryAssets(filter(notExistingProperty));

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("Query assets with query spec where the value (=rightOperand) does not exist")
    void query_nonExistValue() {
        var asset = getAsset("id1");
        asset.getProperties().put("someprop", "someval");
        getAssetIndex().create(asset);
        var notExistingValue = new Criterion("someprop", "=", "some-other-val");

        var assets = getAssetIndex().queryAssets(filter(notExistingValue));

        assertThat(assets).isEmpty();
    }

    @Test
    @DisplayName("Verifies an asset query, that contains a filter expression")
    void query_withFilterExpression() {
        var expected = createAssetBuilder("id1").property("version", "2.0").property("contentType", "whatever").build();
        var differentVersion = createAssetBuilder("id2").property("version", "2.1").property("contentType", "whatever").build();
        var differentContentType = createAssetBuilder("id3").property("version", "2.0").property("contentType", "different").build();
        getAssetIndex().create(expected);
        getAssetIndex().create(differentVersion);
        getAssetIndex().create(differentContentType);
        var filter = filter(
                new Criterion("version", "=", "2.0"),
                new Criterion("contentType", "=", "whatever")
        );

        var assets = getAssetIndex().queryAssets(filter);

        assertThat(assets).hasSize(1).usingRecursiveFieldByFieldElementComparator().containsOnly(expected);
    }

    @Test
    void query_shouldFilterByNestedProperty() {
        var nested = EDC_NAMESPACE + "nested";
        var version = EDC_NAMESPACE + "version";
        var expected = createAssetBuilder("id1").property(nested, Map.of(version, "2.0")).build();
        var differentVersion = createAssetBuilder("id2").property(nested, Map.of(version, "2.1")).build();
        getAssetIndex().create(expected);
        getAssetIndex().create(differentVersion);

        var assets = getAssetIndex().queryAssets(filter(criterion("'%s'.'%s'".formatted(nested, version), "=", "2.0")));

        assertThat(assets).hasSize(1).usingRecursiveFieldByFieldElementComparator().containsOnly(expected);
    }

    @Test
    @DisplayName("Verify an asset query based on an Asset property, where the property value is actually a complex object")
    void query_assetPropertyAsObject() {
        var nested = Map.of("text", "test123", "number", 42, "bool", false);
        var dataAddress = createDataAddress();
        var asset = createAssetBuilder("id1")
                .dataAddress(dataAddress)
                .property("testobj", nested)
                .build();
        getAssetIndex().create(asset);

        var assetsFound = getAssetIndex().queryAssets(QuerySpec.Builder.newInstance()
                .filter(criterion("testobj", "like", "%test1%"))
                .build());

        assertThat(assetsFound).hasSize(1).first().usingRecursiveComparison().isEqualTo(asset);
    }

    @Test
    void query_multipleFound() {
        var testAsset1 = createAsset("foobar");
        var testAsset2 = createAsset("barbaz");
        var testAsset3 = createAsset("barbaz");
        getAssetIndex().create(testAsset1);
        getAssetIndex().create(testAsset2);
        getAssetIndex().create(testAsset3);
        var criterion = new Criterion(Asset.PROPERTY_NAME, "=", "barbaz");

        var assets = getAssetIndex().queryAssets(filter(criterion));

        assertThat(assets).hasSize(2).map(Asset::getId).containsExactlyInAnyOrder(testAsset2.getId(), testAsset3.getId());
    }

    @Test
    @DisplayName("Query assets using the IN operator")
    void query_in() {
        getAssetIndex().create(getAsset("id1"));
        getAssetIndex().create(getAsset("id2"));
        var criterion = new Criterion(Asset.PROPERTY_ID, "in", List.of("id1", "id2"));

        var assetsFound = getAssetIndex().queryAssets(filter(criterion));

        assertThat(assetsFound).isNotNull().hasSize(2);
    }

    @Test
    @DisplayName("Query assets using the IN operator, invalid right operand")
    void query_shouldThrowException_whenOperatorInAndInvalidRightOperand() {
        var asset1 = getAsset("id1");
        getAssetIndex().create(asset1);
        var asset2 = getAsset("id2");
        getAssetIndex().create(asset2);
        var invalidRightOperand = new Criterion(Asset.PROPERTY_ID, "in", "(id1, id2)");

        assertThatThrownBy(() -> getAssetIndex().queryAssets(filter(invalidRightOperand)).toList())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void query_withSorting() {
        var assets = IntStream.range(9, 12)
                .mapToObj(i -> createAsset("test-asset", "id" + i))
                .peek(a -> getAssetIndex().create(a))
                .toList();
        var spec = QuerySpec.Builder.newInstance()
                .sortField(Asset.PROPERTY_ID)
                .sortOrder(SortOrder.ASC)
                .build();

        var result = getAssetIndex().queryAssets(spec);

        assertThat(result).usingRecursiveFieldByFieldElementComparator().containsAll(assets);
    }

    @Test
    void query_withPrivateSorting() {
        var assets = IntStream.range(0, 10)
                .mapToObj(i -> createAssetBuilder(String.valueOf(i)).privateProperty("pKey", "pValue").build())
                .peek(a -> getAssetIndex().create(a))
                .collect(Collectors.toList());

        var spec = QuerySpec.Builder.newInstance().sortField("pKey").sortOrder(SortOrder.ASC).build();

        var result = getAssetIndex().queryAssets(spec);

        assertThat(result).usingRecursiveFieldByFieldElementComparator().containsAll(assets);
    }

    @Test
    @DisplayName("Query assets using the LIKE operator")
    void query_like() {
        var asset1 = getAsset("id1");
        getAssetIndex().create(asset1);
        var asset2 = getAsset("id2");
        getAssetIndex().create(asset2);
        var criterion = new Criterion(Asset.PROPERTY_ID, "LIKE", "id%");

        var assetsFound = getAssetIndex().queryAssets(filter(criterion));

        assertThat(assetsFound).isNotNull().hasSize(2);
    }

    @Test
    @DisplayName("Query assets using the LIKE operator on a json value")
    void query_likeJson() throws JsonProcessingException {
        var asset = getAsset("id1");
        var nested = Map.of("text", "test123", "number", 42, "bool", false);
        asset.getProperties().put("myjson", new ObjectMapper().writeValueAsString(nested));
        getAssetIndex().create(asset);
        var criterion = new Criterion("myjson", "LIKE", "%test123%");

        var assetsFound = getAssetIndex().queryAssets(filter(criterion));

        assertThat(assetsFound).usingRecursiveFieldByFieldElementComparator().containsExactly(asset);
    }

    @Test
    @DisplayName("Query assets using two criteria, each with the LIKE operator on a nested json value")
    void query_likeJson_withComplexObject() throws JsonProcessingException {
        var asset = getAsset("id1");
        var jsonObject = Map.of("root", Map.of("key1", "value1", "nested1", Map.of("key2", "value2", "key3", Map.of("theKey", "theValue, this is what we're looking for"))));
        asset.getProperties().put("myProp", new ObjectMapper().writeValueAsString(jsonObject));
        getAssetIndex().create(asset);
        var criterion1 = new Criterion("myProp", "LIKE", "%is%what%");
        var criterion2 = new Criterion("myProp", "LIKE", "%we're%looking%");

        var assetsFound = getAssetIndex().queryAssets(filter(criterion1, criterion2));

        assertThat(assetsFound).usingRecursiveFieldByFieldElementComparator().containsExactly(asset);
    }

    @Test
    void query_shouldReturnAsset() {
        var id = UUID.randomUUID().toString();
        var asset = getAsset(id);
        getAssetIndex().create(asset);

        var assetFound = getAssetIndex().findById(id);

        assertThat(assetFound).isNotNull();
        assertThat(assetFound).usingRecursiveComparison().isEqualTo(asset);
    }

    @Test
    void query_shouldReturnNull_whenAssetDoesNotExist() {
        var result = getAssetIndex().findById("unexistent");

        assertThat(result).isNull();
    }

    @Test
    @DisplayName("Find a data address that doesn't exist")
    void query_resove_doesNotExist() {
        assertThat(getAssetIndex().resolveForAsset("id1")).isNull();
    }

    @Test
    @DisplayName("Find a data address that exists")
    void resolve_exists() {
        var asset = getAsset("id1");
        var dataAddress = getDataAddress();
        getAssetIndex().create(asset);

        var dataAddressFound = getAssetIndex().resolveForAsset("id1");

        assertThat(dataAddressFound).isNotNull();
        assertThat(dataAddressFound).usingRecursiveComparison().isEqualTo(dataAddress);
    }

    @NotNull
    protected Asset createAsset(String name) {
        return createAsset(name, UUID.randomUUID().toString());
    }

    @NotNull
    protected Asset createAsset(String name, String id) {
        return createAsset(name, id, "contentType");
    }

    @NotNull
    protected Asset createAsset(String name, String id, String contentType) {
        return Asset.Builder.newInstance()
                .id(id)
                .name(name)
                .version("1")
                .contentType(contentType)
                .dataAddress(DataAddress.Builder.newInstance()
                        .keyName("test-keyname")
                        .type(contentType)
                        .build())
                .build();
    }

    protected DataAddress createDataAddress() {
        return DataAddress.Builder.newInstance()
                .keyName("test-keyname")
                .type("type")
                .build();
    }

    protected Asset.Builder createAssetBuilder(String id) {
        return Asset.Builder.newInstance()
                .id(id)
                .createdAt(Clock.systemUTC().millis())
                .property("key" + id, "value" + id)
                .contentType("type")
                .dataAddress(getDataAddress());
    }

    protected AssetIndex getAssetIndex() {
        return sqlAssetIndex;
    }

    private QuerySpec filter(Criterion... criteria) {
        return QuerySpec.Builder.newInstance().filter(Arrays.asList(criteria)).build();
    }

    private Asset getAsset(String id) {
        return createAssetBuilder(id)
                .build();
    }

    private DataAddress getDataAddress() {
        return DataAddress.Builder.newInstance()
                .type("type")
                .property("key", "value")
                .build();
    }

    @BeforeAll
    static void prepare(GaussDbTestExtension.SqlHelper runner) throws IOException {
        runner.executeStatement(Files.readString(Paths.get("docs/schema.sql")));
    }

    @AfterAll
    static void deleteTable(GaussDbTestExtension.SqlHelper runner) {
        runner.dropTable(SQL_STATEMENTS.getAssetTable());
    }
}