package com.hmsonline.trident.cql;

import com.hmsonline.trident.cql.incremental.CassandraCqlIncrementalState;
import com.hmsonline.trident.cql.incremental.CassandraCqlIncrementalStateFactory;
import com.hmsonline.trident.cql.incremental.CassandraCqlIncrementalStateUpdater;
import com.hmsonline.trident.cql.incremental.example.SalesAnalyticsMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.State;
import storm.trident.testing.MockTridentTuple;
import storm.trident.tuple.TridentTuple;

import java.util.*;

/**
 * Test that demonstrates how to construct and use conditional updates.
 */
@RunWith(JUnit4.class)
public class IncrementalStateTest {
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalStateTest.class);
    private CassandraCqlIncrementalStateFactory<String, Number> stateFactory;
    private CassandraCqlIncrementalStateUpdater<String, Number> stateUpdater;
    private Map configuration;
    private static List<String> FIELDS = Arrays.asList("price", "state", "product");

    private CqlClientFactory clientFactory;

    public IncrementalStateTest() {
        SalesAnalyticsMapper mapper = new SalesAnalyticsMapper();
        stateFactory = new CassandraCqlIncrementalStateFactory<String, Number>(new Sum(), mapper);
        stateUpdater = new CassandraCqlIncrementalStateUpdater<String, Number>();
        configuration = new HashMap<String, String>();
        configuration.put(CassandraCqlStateFactory.TRIDENT_CASSANDRA_CQL_HOSTS, "localhost");
    }

    @Test
    public void testStateUpdates() throws Exception {
        // Let's get some initial state in the database
        CassandraCqlIncrementalState<String, Number> state = (CassandraCqlIncrementalState<String, Number>)
                stateFactory.makeState(configuration, null, 0, 0);
        MockTridentTuple mockTuple = new MockTridentTuple(FIELDS, Arrays.asList(100, "MD", "bike"));
        List<TridentTuple> mockTuples = new ArrayList<TridentTuple>();
        mockTuples.add(mockTuple);

        state.beginCommit(Long.MAX_VALUE);
        stateUpdater.updateState(state, mockTuples, null);
        state.commit(Long.MAX_VALUE);


        // Let's create two state objects, to simulate multi-threaded/distributed operations.
        State state1 = stateFactory.makeState(configuration, null, 1, 2);
        State state2 = stateFactory.makeState(configuration, null, 2, 2);

    }
}
