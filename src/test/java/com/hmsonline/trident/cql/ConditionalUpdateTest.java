package com.hmsonline.trident.cql;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static com.hmsonline.trident.cql.incremental.example.SalesAnalyticsMapper.KEYSPACE_NAME;
import static com.hmsonline.trident.cql.incremental.example.SalesAnalyticsMapper.KEY_NAME;
import static com.hmsonline.trident.cql.incremental.example.SalesAnalyticsMapper.TABLE_NAME;
import static com.hmsonline.trident.cql.incremental.example.SalesAnalyticsMapper.VALUE_NAME;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.datastax.driver.core.querybuilder.Update;

/**
 * Test that demonstrates how to construct and use conditional updates.
 */
@RunWith(JUnit4.class)
public class ConditionalUpdateTest extends CqlTestEnvironment {
    //private static final Logger LOG = LoggerFactory.getLogger(ConditionalUpdateTest.class);
    public String APPLIED_COLUMN = "[applied]";

    public ConditionalUpdateTest() {
        super();
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
