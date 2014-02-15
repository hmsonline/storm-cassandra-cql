package com.hmsonline.trident.cql;

import com.datastax.driver.core.querybuilder.Delete;
import com.hmsonline.trident.cql.incremental.CassandraCqlIncrementalState;
import com.hmsonline.trident.cql.incremental.CassandraCqlIncrementalStateFactory;
import com.hmsonline.trident.cql.incremental.CassandraCqlIncrementalStateUpdater;
import com.hmsonline.trident.cql.incremental.example.SalesAnalyticsMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MockTridentTuple;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.hmsonline.trident.cql.incremental.example.SalesAnalyticsMapper.*;

/**
 * Test that demonstrates how to construct and use conditional updates.
 */
@RunWith(JUnit4.class)
public class IncrementalStateTest extends ConditionalUpdateTest {
    private CassandraCqlIncrementalStateFactory<String, Number> stateFactory;
    private CassandraCqlIncrementalStateUpdater<String, Number> stateUpdater;
    private static List<String> FIELDS = Arrays.asList("price", "state", "product");

    public IncrementalStateTest() {
        SalesAnalyticsMapper mapper = new SalesAnalyticsMapper();
        stateFactory = new CassandraCqlIncrementalStateFactory<String, Number>(new Sum(), mapper);
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
        CassandraCqlIncrementalState<String, Number> state = (CassandraCqlIncrementalState<String, Number>)
                stateFactory.makeState(configuration, null, 5, 50);
        this.incrementState(state);
        state.commit(Long.MAX_VALUE);
        this.assertValue("MD", 100);

        // Let's create two state objects, to simulate multi-threaded/distributed operations.
        CassandraCqlIncrementalState<String, Number> state1 = (CassandraCqlIncrementalState<String, Number>)
                stateFactory.makeState(configuration, null, 55, 122);
        this.incrementState(state1);
        state.commit(Long.MAX_VALUE);
        this.assertValue("MD", 200);
    }

    @SuppressWarnings("unchecked")
    private void incrementState(CassandraCqlIncrementalState<String, Number> state) {
        MockTridentTuple mockTuple = new MockTridentTuple(FIELDS, Arrays.asList(100, "MD", "bike"));
        List<TridentTuple> mockTuples = new ArrayList<TridentTuple>();
        mockTuples.add(mockTuple);
        state.beginCommit(Long.MAX_VALUE);
        stateUpdater.updateState(state, mockTuples, null);
    }
}
