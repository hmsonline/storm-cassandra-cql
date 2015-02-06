package com.hmsonline.trident.cql;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.hmsonline.trident.cql.incremental.example.SalesAnalyticsMapper.KEYSPACE_NAME;
import static com.hmsonline.trident.cql.incremental.example.SalesAnalyticsMapper.KEY_NAME;
import static com.hmsonline.trident.cql.incremental.example.SalesAnalyticsMapper.TABLE_NAME;
import static com.hmsonline.trident.cql.incremental.example.SalesAnalyticsMapper.VALUE_NAME;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Before;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Select;

public class CqlTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(CqlTestEnvironment.class);

    public CqlClientFactory clientFactory;
    public Map<Object, Object> configuration = new HashMap<Object, Object>();
    
    @Rule
    public static CassandraCQLUnit cqlUnit;

    public CqlTestEnvironment() {
        configuration.put(MapConfiguredCqlClientFactory.TRIDENT_CASSANDRA_CQL_HOSTS, "localhost");
        clientFactory = new CqlUnitClientFactory(configuration, cqlUnit);
    }

    public Session getSession(){
        return clientFactory.getSession();
    }
    
    @Before
    public void setup(){
        if (cqlUnit == null){
            cqlUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("schema.cql","mykeyspace"));            
        }
    }
    
    public void assertValue(String k, Integer expectedValue) {
        Select selectStatement = select().column("v").from(KEYSPACE_NAME, TABLE_NAME);
        selectStatement.where(eq(KEY_NAME, k));
        ResultSet results = getSession().execute(selectStatement);
        Integer actualValue = results.one().getInt(VALUE_NAME);
        assertEquals(expectedValue, actualValue);
    }

    public void executeAndAssert(Statement statement, String k, Integer expectedValue) {
        LOG.debug("EXECUTING [{}]", statement.toString());
        ResultSet results = getSession().execute(statement);
        Row row = results.one();
        if (row != null)
            LOG.debug("APPLIED?[{}]", row.getBool("[applied]"));
        assertValue(k, expectedValue);
    }
}
