package com.hmsonline.trident.cql;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;

import com.datastax.driver.core.Statement;

public class CassandraCqlStateUpdater implements StateUpdater<CassandraCqlState> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CassandraCqlStateUpdater.class);
    private CqlTupleMapper mapper = null; 
    
    public CassandraCqlStateUpdater(CqlTupleMapper mapper){
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
    	for (TridentTuple tuple : tuples){
    		Statement statement = this.mapper.map(tuple);
    		state.addStatement(statement);
        }
    }
}