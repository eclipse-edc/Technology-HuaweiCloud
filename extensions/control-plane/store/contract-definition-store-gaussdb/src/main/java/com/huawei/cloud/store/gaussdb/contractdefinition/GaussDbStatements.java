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

package com.huawei.cloud.store.gaussdb.contractdefinition;


import org.eclipse.edc.connector.controlplane.store.sql.contractdefinition.schema.postgres.ContractDefinitionMapping;
import org.eclipse.edc.connector.controlplane.store.sql.contractdefinition.schema.postgres.PostgresDialectStatements;
import org.eclipse.edc.spi.query.QuerySpec;
import org.eclipse.edc.sql.translation.SqlQueryStatement;

public class GaussDbStatements extends PostgresDialectStatements {

    public static final String ASSETS_SELECTOR_PREFIX = "assetsSelector.";

    @Override
    public SqlQueryStatement createQuery(QuerySpec querySpec) {

        if (querySpec.containsAnyLeftOperand(ASSETS_SELECTOR_PREFIX)) {
            var select = "SELECT * FROM %s ".formatted(getContractDefinitionTable());

            var criteria = querySpec.getFilterExpression();

            // contains only criteria that target the assetSelector, i.e. will use JSON-query syntax
            var filteredCriteria = criteria.stream()
                    .filter(c -> c.getOperandLeft().toString().startsWith(ASSETS_SELECTOR_PREFIX))
                    .toList();

            // remove all criteria, that target the assetSelector
            criteria.removeAll(filteredCriteria);

            var stmt = new SqlQueryStatement(select, querySpec, new ContractDefinitionMapping(this), operatorTranslator);

            // manually construct a SELECT statement using json_array_elements syntax.
            // for reference, check this article: https://stackoverflow.com/a/30691077/7079724
            filteredCriteria.forEach(
                    fc -> {
                        var sanitizedLeftOp = fc.getOperandLeft().toString().replace(ASSETS_SELECTOR_PREFIX, "");
                        stmt.addWhereClause("? IN (SELECT json_array_elements( %s.%s ) ->> ?)".formatted(getContractDefinitionTable(), getAssetsSelectorColumn()), fc.getOperandRight(), sanitizedLeftOp);
                    });
            return stmt;
        }
        return super.createQuery(querySpec);
    }
}
