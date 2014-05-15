package com.hmsonline.trident.cql.incremental;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.hmsonline.trident.cql.CqlClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.CombinerAggregator;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CassandraCqlIncrementalState<K, V> implements State {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraCqlIncrementalState.class);
    private CqlClientFactory clientFactory;
    private CombinerAggregator<V> aggregator;
    private CqlIncrementMapper<K, V> mapper;
    private Map<K, V> aggregateValues;
    public static int MAX_ATTEMPTS = 10;
    private int partitionIndex;

    public CassandraCqlIncrementalState(CqlClientFactory clientFactory, CombinerAggregator<V> aggregator,
            CqlIncrementMapper<K, V> mapper, int partitionIndex) {
        this.clientFactory = clientFactory;
        this.aggregator = aggregator;
        this.mapper = mapper;
        this.partitionIndex = partitionIndex;
    }

    @Override
    public void beginCommit(Long txid) {
        aggregateValues = new HashMap<K, V>();
    }

    private boolean applyUpdate(Statement updateStatement) {
        LOG.debug("APPLYING [{}]", updateStatement.toString());
        ResultSet results = clientFactory.getSession().execute(updateStatement);
        Row row = results.one();
        if (row != null) {
            return row.getBool("[applied]");
        }
        return false;
    }

    @Override
    public void commit(Long txid) {
        boolean applied = false;
        int attempts = 0;
        // Read current value.
        //if we failed to apply the update , maybe the state has change already , we need to calculate the new state and apply it again
        while (!applied && attempts < MAX_ATTEMPTS) {
            for (Map.Entry<K, V> entry : aggregateValues.entrySet()) {

                Statement readStatement = mapper.read(entry.getKey());
                LOG.debug("EXECUTING [{}]", readStatement.toString());

                ResultSet results = clientFactory.getSession().execute(readStatement);

                if (results != null) {
                    List<Row> rows = results.all();
                    PersistedState<V> persistedState = mapper.currentState(entry.getKey(), rows);
                    LOG.debug("Persisted value = [{}]", persistedState.getValue());

                    V combinedValue;
                    if (persistedState.getValue() != null)
                        combinedValue = aggregator.combine(entry.getValue(), persistedState.getValue());
                    else
                        combinedValue = entry.getValue();

                    Statement updateStatement = mapper.update(entry.getKey(), combinedValue, persistedState, txid,
                            partitionIndex);
                    applied = applyUpdate(updateStatement);
                }
            }
            attempts++;
        }
    }

    // TODO: Do we need to synchronize this? (or use Concurrent)
    public void aggregateValue(TridentTuple tuple) {
        K key = mapper.getKey(tuple);
        V value = mapper.getValue(tuple);
        V currentValue = aggregateValues.get(key);
        V newValue;
        if (currentValue == null) {
            newValue = aggregator.init(tuple);
        } else {
            newValue = aggregator.combine(currentValue, value);
        }
        LOG.debug("Updating state [{}] ==> [{}]", new Object[]{key, newValue});
        aggregateValues.put(key, newValue);
    }

}
