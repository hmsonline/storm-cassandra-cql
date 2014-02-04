package com.hmsonline.trident.cql.incremental;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.hmsonline.trident.cql.CqlClientFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

public class CassandraCqlIncrementalState<K, V> implements State {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraCqlIncrementalState.class);
    private CqlClientFactory clientFactory;
    private CombinerAggregator<V> aggregator;
    private CqlIncrementMapper<K, V> mapper;
    private Map<K, V> aggregateValues;

    public CassandraCqlIncrementalState(CqlClientFactory clientFactory, CombinerAggregator<V> aggregator, CqlIncrementMapper<K, V> mapper) {
        this.clientFactory = clientFactory;
        this.aggregator = aggregator;
        this.mapper = mapper;
    }

    @Override
    public void beginCommit(Long txid) {
        aggregateValues = new HashMap<K, V>();
    }

    @Override
    public void commit(Long txid) {
        // Read current value.
        for (Map.Entry<K, V> entry : aggregateValues.entrySet()) {
            Statement readStatement = mapper.read(entry.getKey());
            ResultSet resultSet = clientFactory.getSession().execute(readStatement);
            V persistedValue = mapper.currentValue(resultSet);
            V combinedValue;
            // TODO: more elegant solution to this issue
            // Must be careful here as the first persisted value might not exist yet!
            if ( persistedValue != null )
            	combinedValue = aggregator.combine(entry.getValue(), persistedValue);
            else 
            	combinedValue = entry.getValue();
            Statement updateStatement = mapper.update(entry.getKey(), combinedValue);
            clientFactory.getSession().execute(updateStatement);
        }
    }

    // TODO: Do we need to synchronize this?
    public void aggregateValue(TridentTuple tuple, TridentCollector collector) {
        K key = mapper.getKey(tuple);
        V value = mapper.getValue(tuple);
        V currentValue = aggregateValues.get(key);
        V newValue = null;
        if (currentValue == null) {
            newValue = aggregator.init(tuple);
        } else {
            newValue = aggregator.combine(currentValue, value);
        }
        LOG.debug("Updating state [{}] ==> [{}]", new Object[]{key, newValue});
        aggregateValues.put(key, newValue);
    }

}
