package com.huawei.cloud.store.gaussdb.policydefinition;


import org.eclipse.edc.connector.store.sql.policydefinition.store.schema.postgres.PolicyDefinitionMapping;
import org.eclipse.edc.connector.store.sql.policydefinition.store.schema.postgres.PostgresDialectStatements;
import org.eclipse.edc.spi.persistence.EdcPersistenceException;
import org.eclipse.edc.spi.query.QuerySpec;
import org.eclipse.edc.sql.translation.SqlQueryStatement;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.lang.String.format;

public class GaussDbStatements extends PostgresDialectStatements {

    public static final String POLICY_PROHIBITIONS = "policy.prohibitions.";
    public static final String POLICY_PERMISSIONS = "policy.permissions.";
    public static final String POLICY_OBLIGATIONS = "policy.obligations.";
    public static final String POLICY_EXTENSIBLE_PROPERTIES = "policy.extensibleProperties.";
    public static final List<String> POLICY_PREFIXES = List.of(POLICY_PROHIBITIONS,
            POLICY_PERMISSIONS,
            POLICY_OBLIGATIONS,
            POLICY_EXTENSIBLE_PROPERTIES);

    public final Map<String, Supplier<String>> prefixColumnMapping = Map.of(
            POLICY_PROHIBITIONS, this::getProhibitionsColumn,
            POLICY_OBLIGATIONS, this::getDutiesColumn,
            POLICY_EXTENSIBLE_PROPERTIES, this::getExtensiblePropertiesColumn,
            POLICY_PERMISSIONS, this::getPermissionsColumn);

    @Override
    public SqlQueryStatement createQuery(QuerySpec querySpec) {

        if (POLICY_PREFIXES.stream().anyMatch(querySpec::containsAnyLeftOperand)) {
            var select = "SELECT * FROM %s ".formatted(getPolicyTable());

            var criteria = querySpec.getFilterExpression();

            // contains only criteria that target the assetSelector, i.e. will use JSON-query syntax
            var filteredCriteria = criteria.stream()
                    .filter(c -> {
                        var operandLeft = c.getOperandLeft().toString();
                        return POLICY_PREFIXES.stream().anyMatch(operandLeft::startsWith);
                    })
                    .toList();

            // remove all criteria, that target the assetSelector
            criteria.removeAll(filteredCriteria);

            var stmt = new SqlQueryStatement(select, querySpec, new PolicyDefinitionMapping(this));

            // manually construct a SELECT statement using json_array_elements syntax.
            // for reference, check this article: https://stackoverflow.com/a/30691077/7079724
            filteredCriteria.forEach(
                    fc -> {
                        var operandLeft = fc.getOperandLeft().toString();
                        var prefix = POLICY_PREFIXES.stream().filter(operandLeft::startsWith)
                                .findFirst()
                                .orElseThrow(() -> new EdcPersistenceException(format("Operand left %s not valid for policy filtering", operandLeft)));

                        var column = prefixColumnMapping.get(prefix).get();
                        var sanitizedLeftOp = fc.getOperandLeft().toString().replace(prefix, "");
                        stmt.addWhereClause("? IN (SELECT json_array_elements( %s.%s ) #>> %s)".formatted(getPolicyTable(), column, jsonPathExpression(sanitizedLeftOp)), fc.getOperandRight());
                    });
            return stmt;
        }
        return super.createQuery(querySpec);
    }

    /**
     * Use the <a href="https://www.postgresql.org/docs/9.5/functions-json.html">#>></a> operator for fetching the nested path
     */
    private String jsonPathExpression(String inputPath) {
        return format("'{%s}'", inputPath.replace(".", ","));
    }
}
