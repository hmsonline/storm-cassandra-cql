package com.hmsonline.trident.cql;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.state.State;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.Statement;

public class CassandraCqlState implements State {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraCqlState.class);

    private CqlClientFactory clientFactory;
    List<Statement> statements = new ArrayList<Statement>();

    public CassandraCqlState(CqlClientFactory clientFactory) {
        this.clientFactory = clientFactory;
    }

	@Override
	public void beginCommit(Long txid) {
	}

	@Override
	public void commit(Long txid) {
		LOG.debug("Commiting [{}]", txid);		
		BatchStatement batch = new BatchStatement(Type.LOGGED);
		batch.addAll(this.statements);
		clientFactory.getSession().execute(batch);
	}

	public void addStatement(Statement statement){
		this.statements.add(statement);
	}
}
