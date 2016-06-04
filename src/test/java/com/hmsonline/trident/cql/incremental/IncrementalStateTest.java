package com.hmsonline.trident.cql.incremental;

import com.datastax.driver.core.querybuilder.Delete;
import com.hmsonline.trident.cql.CqlTestEnvironment;
import com.hmsonline.trident.cql.example.sales.SalesMapper;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.tuple.TridentTupleView;
import org.apache.storm.tuple.Fields;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.hmsonline.trident.cql.example.sales.SalesMapper.*;

/**
 * Test that demonstrates how to construct and use conditional updates.
 */
@RunWith(JUnit4.class)
public class IncrementalStateTest extends CqlTestEnvironment {
    private CassandraCqlIncrementalStateFactory<String, Number> stateFactory;
    private CassandraCqlIncrementalStateUpdater<String, Number> stateUpdater;
    private static Fields FIELDS = new Fields("price", "state", "product");

    public IncrementalStateTest() {
        super();
        SalesMapper mapper = new SalesMapper();
        stateFactory = new CassandraCqlIncrementalStateFactory<String, Number>(new Sum(), mapper);
        stateFactory.setCqlClientFactory(clientFactory);
        stateUpdater = new CassandraCqlIncrementalStateUpdater<String, Number>();
    }

    private void clearState() {
        Delete deleteStatement = delete().all().from(KEYSPACE_NAME, TABLE_NAME);
        deleteStatement.where(eq(KEY_NAME, "MD"));
        clientFactory.getSession().execute(deleteStatement);
    }
 
    @SuppressWarnings("unchecked")
    @Test
    public void testStateUpdates() throws Exception {
        clearState();

        // Let's get some initial state in the database

        CassandraCqlIncrementalState<String, Number> state = 
                (CassandraCqlIncrementalState<String, Number>) stateFactory.makeState(configuration, null, 5, 50);
        incrementState(state);
        assertValue("MD", 100);

        CassandraCqlIncrementalState<String, Number> state1 =
                (CassandraCqlIncrementalState<String, Number>) stateFactory.makeState(configuration, null, 55, 122);
        incrementState(state1);
        assertValue("MD", 200);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testValueAggregations() {
        clearState();
        CassandraCqlIncrementalState<String, Number> state = (CassandraCqlIncrementalState<String, Number>) stateFactory.makeState(configuration, null, 5, 50);
        state.beginCommit(Long.MAX_VALUE);
        state.aggregateValue(TridentTupleView.createFreshTuple(FIELDS, 100, "MD", "bike"));
        state.aggregateValue(TridentTupleView.createFreshTuple(FIELDS, 50, "PA", "bike"));
        state.aggregateValue(TridentTupleView.createFreshTuple(FIELDS, 10, "PA", "bike"));
        state.commit(Long.MAX_VALUE);
        assertValue("MD", 100);
        assertValue("PA", 60);
    }

    private void incrementState(CassandraCqlIncrementalState<String, Number> state) {
        TridentTuple mockTuple = TridentTupleView.createFreshTuple(FIELDS, 100, "MD", "bike");
        List<TridentTuple> mockTuples = new ArrayList<TridentTuple>();
        mockTuples.add(mockTuple);
        state.beginCommit(Long.MAX_VALUE);
        stateUpdater.updateState(state, mockTuples, null);
        state.commit(Long.MAX_VALUE);
    }
}
