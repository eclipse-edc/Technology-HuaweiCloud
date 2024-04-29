package com.huawei.cloud.store.gaussdb.transferprocess;

import org.eclipse.edc.connector.controlplane.store.sql.transferprocess.store.schema.postgres.PostgresDialectStatements;
import org.eclipse.edc.connector.controlplane.store.sql.transferprocess.store.schema.postgres.TransferProcessMapping;
import org.eclipse.edc.spi.query.QuerySpec;
import org.eclipse.edc.sql.translation.SqlQueryStatement;

import static java.lang.String.format;
import static org.eclipse.edc.sql.dialect.PostgresDialect.getSelectFromJsonArrayTemplate;

public class GaussDbStatements extends PostgresDialectStatements {
    public static final String DEPROVISIONED_RESOURCES_ALIAS = "dpr"; //must be different from column name to avoid ambiguities
    public static final String DEPROVISIONED_RESOURCES_PREFIX = "deprovisionedResources.";
    public static final String RESOURCE_MANIFEST_DEFINITIONS_PREFIX = "resourceManifest.definitions.";
    public static final String PROVISIONED_RESOURCE_SET_RESOURCES_PREFIX = "provisionedResourceSet.resources.";
    private static final String RESOURCES_ALIAS = "resources";
    private static final String DEFINITIONS_ALIAS = "definitions";

    @Override
    public SqlQueryStatement createQuery(QuerySpec querySpec) {
        // if any criterion targets a JSON array field, we need to slightly adapt the FROM clause
        if (querySpec.containsAnyLeftOperand(RESOURCE_MANIFEST_DEFINITIONS_PREFIX)) {
            var select = getSelectFromJsonArrayTemplate(getSelectTemplate(), format("%s -> '%s'", getResourceManifestColumn(), "definitions"), DEFINITIONS_ALIAS);
            return new SqlQueryStatement(select, querySpec, new TransferProcessMapping(this), operatorTranslator);
        } else if (querySpec.containsAnyLeftOperand(PROVISIONED_RESOURCE_SET_RESOURCES_PREFIX)) {
            var select = getSelectFromJsonArrayTemplate(getSelectTemplate(), format("%s -> '%s'", getProvisionedResourceSetColumn(), "resources"), RESOURCES_ALIAS);
            return new SqlQueryStatement(select, querySpec, new TransferProcessMapping(this), operatorTranslator);
        } else if (querySpec.containsAnyLeftOperand(DEPROVISIONED_RESOURCES_PREFIX)) {
            var select = getSelectFromJsonArrayTemplate(getSelectTemplate(), format("%s", getDeprovisionedResourcesColumn()), DEPROVISIONED_RESOURCES_ALIAS);
            return new SqlQueryStatement(select, querySpec, new TransferProcessMapping(this), operatorTranslator);
        }
        return super.createQuery(querySpec);
    }
}
