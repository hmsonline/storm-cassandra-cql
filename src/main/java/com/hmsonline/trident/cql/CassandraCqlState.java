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
    private CqlClientFactory clientFactory;
    private int maxBatchSize;
    private ConsistencyLevel batchConsistencyLevel;
    List<Statement> statements = new ArrayList<Statement>();
    
    public CassandraCqlState(CqlClientFactory clientFactory, ConsistencyLevel batchConsistencyLevel) {
        this(clientFactory, CassandraCqlStateFactory.DEFAULT_MAX_BATCH_SIZE, batchConsistencyLevel);
    }
    
    public CassandraCqlState(CqlClientFactory clientFactory, int maxBatchSize, ConsistencyLevel batchConsistencyLevel) {
        this.clientFactory = clientFactory;
        this.maxBatchSize = maxBatchSize;
        this.batchConsistencyLevel = batchConsistencyLevel;
    }

    @Override
    public void beginCommit(Long txid) {
    }

    @Override
    public void commit(Long txid) {
        LOG.debug("Commiting [{}]", txid);
        BatchStatement batch = new BatchStatement(Type.LOGGED);
        batch.setConsistencyLevel(batchConsistencyLevel);
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
        this.statements.clear();
    }

    public void addStatement(Statement statement) {
        this.statements.add(statement);
    }
    
    public ResultSet execute(Statement statement){
        return clientFactory.getSession().execute(statement);
    }
}
