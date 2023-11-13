package com.huawei.cloud.store.gaussdb.contractdefinition;

import org.eclipse.edc.spi.query.Criterion;
import org.eclipse.edc.spi.query.QuerySpec;
import org.eclipse.edc.spi.query.SortOrder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GaussDbStatementsTest {

    public static final GaussDbStatements STATEMENTS = new GaussDbStatements();

    @Test
    void createQuery_withAssetSelector_multipleCriteria() {
        var query = QuerySpec.Builder.newInstance()
                .filter(new Criterion("assetsSelector.rightOperand", "=", "foobar"))
                .filter(new Criterion("id", "=", "id0"))
                .filter(new Criterion("assetsSelector.operator", "=", "="))
                .build();

        var stmt = STATEMENTS.createQuery(query);
        assertThat(stmt.getQueryAsString()).isEqualToIgnoringWhitespace("SELECT * " +
                "FROM edc_contract_definitions " +
                "WHERE contract_definition_id = ? " +
                "AND ? IN (SELECT json_array_elements(edc_contract_definitions.assets_selector) ->> ?) " +
                "AND ? IN (SELECT json_array_elements(edc_contract_definitions.assets_selector) ->> ?) " +
                "LIMIT ? OFFSET ?;");
        assertThat(stmt.getParameters()).hasSize(7)
                .containsExactly("id0", "foobar", "rightOperand", "=", "operator", 50, 0);
    }

    @Test
    void createQuery_withSingleCriterion() {
        var query = QuerySpec.Builder.newInstance()
                .filter(new Criterion("id", "=", "id0"))
                .build();
        var stmt = STATEMENTS.createQuery(query);
        assertThat(stmt.getQueryAsString()).isEqualToIgnoringWhitespace("SELECT * FROM edc_contract_definitions WHERE contract_definition_id = ? LIMIT ? OFFSET ?;");
        assertThat(stmt.getParameters()).containsExactly("id0", 50, 0);
    }

    @Test
    void createQuery_withAssetSelector_withSortAndOrder() {
        var query = QuerySpec.Builder.newInstance()
                .filter(new Criterion("assetsSelector.rightOperand", "=", "foobar"))
                .sortField("accessPolicyId")
                .sortOrder(SortOrder.DESC)
                .build();

        var stmt = STATEMENTS.createQuery(query);
        assertThat(stmt.getQueryAsString()).isEqualToIgnoringWhitespace("SELECT * " +
                "FROM edc_contract_definitions " +
                "WHERE ? IN (SELECT json_array_elements(edc_contract_definitions.assets_selector) ->> ?) " +
                "ORDER BY access_policy_id DESC" +
                "LIMIT ? OFFSET ?;");
        assertThat(stmt.getParameters()).hasSize(4)
                .containsExactly("foobar", "rightOperand", 50, 0);
    }
}