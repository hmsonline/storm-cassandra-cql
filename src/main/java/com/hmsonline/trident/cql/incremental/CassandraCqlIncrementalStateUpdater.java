package com.hmsonline.trident.cql.incremental;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.StateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;

public class CassandraCqlIncrementalStateUpdater<K, V> implements StateUpdater<CassandraCqlIncrementalState<K, V>> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CassandraCqlIncrementalStateUpdater.class);

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map configuration, TridentOperationContext context) {
        LOG.debug("Preparing updater with [{}]", configuration);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void updateState(CassandraCqlIncrementalState<K, V> state, List<TridentTuple> tuples,
                            TridentCollector collector) {
        for (TridentTuple tuple : tuples) {
            state.aggregateValue(tuple);
        }
    }
}