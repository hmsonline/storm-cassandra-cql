package com.hmsonline.trident.cql;

import com.datastax.driver.core.Statement;
import com.hmsonline.trident.cql.mappers.CqlTupleMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.StateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;

public class CassandraCqlStateUpdater<K,V> implements StateUpdater<CassandraCqlState> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CassandraCqlStateUpdater.class);
    private CqlTupleMapper<K,V> mapper = null;
	private boolean propagateTuples;

    public CassandraCqlStateUpdater(CqlTupleMapper<K,V> mapper) {
        this(mapper, false);
    }

	public CassandraCqlStateUpdater(CqlTupleMapper<K,V> mapper, boolean propagateTuples) {
        this.mapper = mapper;
		this.propagateTuples = propagateTuples;
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
			if (propagateTuples) {
				collector.emit(tuple);
			}
        }
    }
}