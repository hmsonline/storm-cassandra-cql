package com.hmsonline.trident.cql.incremental;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;

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