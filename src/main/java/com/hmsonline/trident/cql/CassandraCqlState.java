package com.hmsonline.trident.cql;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.state.State;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;

public class CassandraCqlState implements State {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraCqlState.class);
    private static final int DEFAULT_MAX_BATCH_SIZE = 100;
    private CqlClientFactory clientFactory;
    private int maxBatchSize;
    private ConsistencyLevel consistencyLevel;
    List<Statement> statements = new ArrayList<Statement>();
    
    public CassandraCqlState(CqlClientFactory clientFactory, ConsistencyLevel consistencyLevel) {
        this(clientFactory, DEFAULT_MAX_BATCH_SIZE, consistencyLevel);
    }
    
    public CassandraCqlState(CqlClientFactory clientFactory, int maxBatchSize, ConsistencyLevel consistencyLevel) {
        this.clientFactory = clientFactory;
        this.maxBatchSize = maxBatchSize;
        this.consistencyLevel = consistencyLevel;
    }

    @Override
    public void beginCommit(Long txid) {
    }

    @Override
    public void commit(Long txid) {
        LOG.debug("Commiting [{}]", txid);
        BatchStatement batch = new BatchStatement(Type.LOGGED);
        batch.setConsistencyLevel(consistencyLevel);
        int i = 0;
        for(Statement statement : this.statements) {
            batch.add(statement);
            i++;
            if(i >= this.maxBatchSize) {
                clientFactory.getSession().execute(batch);
                batch = new BatchStatement(Type.LOGGED);
                i = 0;
            }
        }
        if(i > 0) {
            clientFactory.getSession().execute(batch);
        }
    }

    public void addStatement(Statement statement) {
        this.statements.add(statement);
    }
    
    public ResultSet execute(Statement statement){
        return clientFactory.getSession().execute(statement);
    }
}
