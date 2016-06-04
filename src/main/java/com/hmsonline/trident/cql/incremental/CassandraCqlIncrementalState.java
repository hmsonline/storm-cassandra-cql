package com.hmsonline.trident.cql.incremental;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.hmsonline.trident.cql.CqlClientFactory;

public class CassandraCqlIncrementalState<K, V> implements State {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraCqlIncrementalState.class);
    private CqlClientFactory clientFactory;
    private CombinerAggregator<V> aggregator;
    private CqlIncrementMapper<K, V> mapper;
    private Map<K, V> aggregateValues;
    public static int MAX_ATTEMPTS = 10;
    private int partitionIndex;
    private int maxAttempts;

    public CassandraCqlIncrementalState(CqlClientFactory clientFactory, CombinerAggregator<V> aggregator,
            CqlIncrementMapper<K, V> mapper, int partitionIndex) {
        init(clientFactory, aggregator, mapper, partitionIndex, MAX_ATTEMPTS);
    }
    
    public CassandraCqlIncrementalState(CqlClientFactory clientFactory, CombinerAggregator<V> aggregator,
            CqlIncrementMapper<K, V> mapper, int partitionIndex, int maxAttempts) {
        init(clientFactory, aggregator, mapper, partitionIndex, maxAttempts);
    }
    
    private void init(CqlClientFactory clientFactory, CombinerAggregator<V> aggregator,
            CqlIncrementMapper<K, V> mapper, int partitionIndex, int maxAttempts) {
        this.clientFactory = clientFactory;
        this.aggregator = aggregator;
        this.mapper = mapper;
        this.partitionIndex = partitionIndex;
        this.maxAttempts = maxAttempts;
    }

    @Override
    public void beginCommit(Long txid) {
        aggregateValues = new HashMap<K, V>();
    }

    private boolean applyUpdate(Statement updateStatement, Long txid) {
        LOG.debug("APPLYING [{}]", updateStatement.toString());
        ResultSet results = clientFactory.getSession().execute(updateStatement);
        Row row = results.one();
        if (row != null) {
            return row.getBool("[applied]");
        } else {
            return true;
        }
    }

    @Override
    public void commit(Long txid) {
        DriverException lastException = null;
        // Read current value.
        //if we failed to apply the update , maybe the state has change already , we need to calculate the new state and apply it again
        for (Map.Entry<K, V> entry : aggregateValues.entrySet()) {
            int attempts = 0;
            boolean applied = false;
            while (!applied && attempts < maxAttempts) {
                try{
                    applied = updateState(entry, txid);
                } catch(QueryExecutionException e) {
                    lastException = e;
                    LOG.warn("Catching {} attempt {}"+txid+"-"+partitionIndex, e.getMessage(), attempts);
                }
                attempts++;
            }
            if(!applied) {
                if(lastException != null) {
                    throw new CassandraCqlIncrementalStateException("Ran out of attempts ["+attempts+"] max of ["+maxAttempts+"] "+txid+"-"+ partitionIndex, lastException);
                } else {
                    throw new CassandraCqlIncrementalStateException("Ran out of attempts ["+attempts+"] max of ["+maxAttempts+"] "+txid+"-"+ partitionIndex);
                }
            }
        }
    }
        
    private boolean updateState(Map.Entry<K, V> entry, Long txid) {
        Statement readStatement = mapper.read(entry.getKey());
        LOG.debug("EXECUTING [{}]", readStatement.toString());

        ResultSet results = clientFactory.getSession().execute(readStatement);
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
        //mapper don't want to update
        if(updateStatement==null){
            return true;
        }
        return applyUpdate(updateStatement, txid);
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
