package com.hmsonline.trident.cql;

import com.datastax.driver.core.Statement;
import com.hmsonline.trident.cql.mappers.CqlTupleMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;

public class CassandraCqlStateUpdater<K,V> implements StateUpdater<CassandraCqlState> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CassandraCqlStateUpdater.class);
    private CqlTupleMapper<K,V> mapper = null;

    public CassandraCqlStateUpdater(CqlTupleMapper<K,V> mapper) {
        this.mapper = mapper;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map configuration, TridentOperationContext context) {
        LOG.debug("Preparing updater with [{}]", configuration);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void updateState(CassandraCqlState state, List<TridentTuple> tuples, TridentCollector collector) {
        for (TridentTuple tuple : tuples) {
            Statement statement = this.mapper.map(tuple);
            state.addStatement(statement);
        }
    }
}