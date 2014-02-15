package com.hmsonline.trident.cql;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.hmsonline.trident.cql.incremental.example.SalesAnalyticsMapper.*;
import static org.junit.Assert.assertEquals;

/**
 * Test that demonstrates how to construct and use conditional updates.
 */
@RunWith(JUnit4.class)
public class ConditionalUpdateTest {
    private static final Logger LOG = LoggerFactory.getLogger(ConditionalUpdateTest.class);
    public CqlClientFactory clientFactory;
    public Map<String, String> configuration;
    public String APPLIED_COLUMN = "[applied]";

    public ConditionalUpdateTest() {
        configuration = new HashMap<String, String>();
        configuration.put(CassandraCqlStateFactory.TRIDENT_CASSANDRA_CQL_HOSTS, "localhost");
        clientFactory = new CqlClientFactory(configuration);
    }

    public void assertValue(String k, Integer expectedValue) {
        Select selectStatement = select().column("v").from(KEYSPACE_NAME, TABLE_NAME);
        selectStatement.where(eq(KEY_NAME, k));
        ResultSet results = clientFactory.getSession().execute(selectStatement);
        Integer actualValue = results.one().getInt(VALUE_NAME);
        assertEquals(expectedValue, actualValue);
    }

    public void executeAndAssert(Statement statement, String k, Integer expectedValue) {
        LOG.debug("EXECUTING [{}]", statement.toString());
        ResultSet results = clientFactory.getSession().execute(statement);
        Row row = results.one();
        if (row != null)
            LOG.debug("APPLIED?[{}]", row.getBool("[applied]"));
        this.assertValue(k, expectedValue);
    }

    @Test
    public void testConditionalUpdates() throws Exception {
        Update initialStatement = update(KEYSPACE_NAME, TABLE_NAME);
        initialStatement.with(set(VALUE_NAME, 10)).where(eq(KEY_NAME, "DE"));
        this.executeAndAssert(initialStatement, "DE", 10);

        // Now let's conditionally update where it is true
        Update updateStatement = update(KEYSPACE_NAME, TABLE_NAME);
        updateStatement.with(set(VALUE_NAME, 15)).where(eq(KEY_NAME, "DE")).onlyIf(eq(VALUE_NAME, 10));
        this.executeAndAssert(updateStatement, "DE", 15);

        // Now let's conditionally update where it is false
        Update conditionalStatement = update(KEYSPACE_NAME, TABLE_NAME);
        conditionalStatement.with(set(VALUE_NAME, 20)).where(eq(KEY_NAME, "DE")).onlyIf(eq(VALUE_NAME, 10));
        this.executeAndAssert(conditionalStatement, "DE", 15);
    }
}
