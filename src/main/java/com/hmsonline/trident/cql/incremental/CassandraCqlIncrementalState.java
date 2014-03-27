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

    public CassandraCqlIncrementalState(CqlClientFactory clientFactory, CombinerAggregator<V> aggregator, CqlIncrementMapper<K, V> mapper, int partitionIndex) {
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
        boolean applied = false;
        int attempts = 0;
        while (!applied && attempts < MAX_ATTEMPTS) {
            ResultSet results = clientFactory.getSession().execute(updateStatement);
            Row row = results.one();
            if (row != null)
                applied = row.getBool("[applied]");
            attempts++;
        }
        return applied;
    }

    @Override
    public void commit(Long txid) {
        // Read current value.
        for (Map.Entry<K, V> entry : aggregateValues.entrySet()) {

            //Try to check pre-condition first; this is optional
            final Statement condition = mapper.condition(entry.getKey(), txid, partitionIndex);
            if (condition != null && !applyUpdate(condition)) {
                continue;
            }

            Statement readStatement = mapper.read(entry.getKey());
            LOG.debug("EXECUTING [{}]", readStatement.toString());

            ResultSet results = clientFactory.getSession().execute(readStatement);

            V persistedValue = null;

            if (results != null) {
                List<Row> rows = results.all();
                if (rows.size() > 1) {
                    LOG.error("Found non-unique value for key [{}]", entry.getKey());
                } else if (rows.size() == 1) {
                    persistedValue = mapper.currentValue(rows.get(0));
                    LOG.debug("Persisted value = [{}]", persistedValue);
                }

                V combinedValue;
                // TODO: more elegant solution to this issue
                // Must be careful here as the first persisted value might not exist yet!
                if (persistedValue != null)
                    combinedValue = aggregator.combine(entry.getValue(), persistedValue);
                else
                    combinedValue = entry.getValue();

                Statement updateStatement = mapper.update(entry.getKey(), combinedValue, persistedValue);
                applyUpdate(updateStatement);
            }

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
