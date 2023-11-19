package com.huawei.cloud.store.gaussdb.transferprocess;

import org.eclipse.edc.connector.store.sql.transferprocess.store.schema.postgres.PostgresDialectStatements;
import org.eclipse.edc.connector.store.sql.transferprocess.store.schema.postgres.TransferProcessMapping;
import org.eclipse.edc.spi.query.Criterion;
import org.eclipse.edc.spi.query.QuerySpec;
import org.eclipse.edc.sql.translation.SqlQueryStatement;
import org.jetbrains.annotations.NotNull;

import java.util.List;

public class GaussDbStatements extends PostgresDialectStatements {
    public static final String DEPROVISIONED_RESOURCES_ALIAS = "dpr"; //must be different from column name to avoid ambiguities
    public static final String DEPROVISIONED_RESOURCES_PREFIX = "deprovisionedResources.";
    public static final String RESOURCE_MANIFEST_DEFINITIONS_PREFIX = "resourceManifest.definitions.";
    public static final String PROVISIONED_RESOURCE_SET_RESOURCES_PREFIX = "provisionedResourceSet.resources.";
    private static final String RESOURCES_ALIAS = "resources";
    private static final String DEFINITIONS_ALIAS = "definitions";

    @Override
    public SqlQueryStatement createQuery(QuerySpec querySpec) {
        var criteria = querySpec.getFilterExpression();

        // queries that target fields, that contain JSON types need to be handled in a special way by sub-selecting from the result of th `json_array_elements` statement.

        if (querySpec.containsAnyLeftOperand(RESOURCE_MANIFEST_DEFINITIONS_PREFIX)) {

            var filteredCriteria = filterLeftOpNotContains(criteria, RESOURCE_MANIFEST_DEFINITIONS_PREFIX);
            var stmt = new SqlQueryStatement(getSelectTemplate(), querySpec, new TransferProcessMapping(this));

            filteredCriteria.forEach(fc -> {
                var sanitizedLeftOp = fc.getOperandLeft().toString().replace(RESOURCE_MANIFEST_DEFINITIONS_PREFIX, "");
                stmt.addWhereClause("? IN (SELECT json_array_elements(%s.%s -> '%s') ->> ?)".formatted(getTransferProcessTableName(), getResourceManifestColumn(), DEFINITIONS_ALIAS), fc.getOperandRight(),
                        sanitizedLeftOp);
            });
            return stmt;
        } else if (querySpec.containsAnyLeftOperand(PROVISIONED_RESOURCE_SET_RESOURCES_PREFIX)) {

            var filteredCriteria = filterLeftOpNotContains(criteria, PROVISIONED_RESOURCE_SET_RESOURCES_PREFIX);
            var stmt = new SqlQueryStatement(getSelectTemplate(), querySpec, new TransferProcessMapping(this));
            filteredCriteria.forEach(fc -> {
                var sanitizedLeftOp = fc.getOperandLeft().toString().replace(PROVISIONED_RESOURCE_SET_RESOURCES_PREFIX, "");
                stmt.addWhereClause("? IN (SELECT json_array_elements((%s.%s ->> '%s')::json) ->> ?)".formatted(getTransferProcessTableName(), getProvisionedResourceSetColumn(), RESOURCES_ALIAS), fc.getOperandRight(),
                        sanitizedLeftOp);
            });
            return stmt;
        } else if (querySpec.containsAnyLeftOperand(DEPROVISIONED_RESOURCES_PREFIX)) {
            var filteredCriteria = filterLeftOpNotContains(criteria, DEPROVISIONED_RESOURCES_PREFIX);
            var stmt = new SqlQueryStatement(getSelectTemplate(), querySpec, new TransferProcessMapping(this));
            filteredCriteria.forEach(fc -> {
                var sanitizedLeftOp = fc.getOperandLeft().toString().replace(DEPROVISIONED_RESOURCES_PREFIX, "");
                stmt.addWhereClause("? IN (SELECT json_array_elements( %s.%s ) ->> ?)".formatted(getTransferProcessTableName(), getDeprovisionedResourcesColumn()), fc.getOperandRight().toString(), sanitizedLeftOp);
            });
            return stmt;
        }
        return super.createQuery(querySpec);
    }

    @Override
    public String getSelectTemplate() {
        return "SELECT *, drq.%s as edc_data_request_id FROM %s LEFT JOIN %s drq ON %s.%s = drq.%s"
                .formatted(getDataRequestIdColumn(), getTransferProcessTableName(), getDataRequestTable(), getTransferProcessTableName(), getIdColumn(), getTransferProcessIdFkColumn());
    }

    @NotNull
    private static List<Criterion> filterLeftOpNotContains(List<Criterion> criteria, String leftOperandPrefix) {
        // contains only criteria that target the deprovisioned resource set, i.e. will use JSON-query syntax
        var filteredCriteria = criteria.stream()
                .filter(c -> c.getOperandLeft().toString().startsWith(leftOperandPrefix))
                .toList();

        // remove all criteria, that target the assetSelector
        criteria.removeAll(filteredCriteria);
        return filteredCriteria;
    }
}
