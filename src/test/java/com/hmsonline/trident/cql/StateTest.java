package com.hmsonline.trident.cql;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Select;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.hmsonline.trident.cql.incremental.example.SalesAnalyticsMapper.*;
import static org.junit.Assert.assertEquals;

public class StateTest {

    private static final Logger LOG = LoggerFactory.getLogger(StateTest.class);

    public CqlClientFactory clientFactory;
    public Map<String, String> configuration = new HashMap<String, String>();

    public StateTest() {
        configuration.put(MapConfiguredCqlClientFactory.TRIDENT_CASSANDRA_CQL_HOSTS, "localhost");
        clientFactory = new MapConfiguredCqlClientFactory(configuration);
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
        assertValue(k, expectedValue);
    }
}
